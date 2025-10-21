// fix_relay_v9_tpd.cpp
// Minimal-diff twin of fix_relay_v9 that uses TCPDirect (zf_*) instead of POSIX sockets.
// Build this as a separate binary for A/B perf vs your existing Onload sockets build.

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include <sched.h>
#include <string>
#include <thread>
#include <vector>

// TCPDirect
extern "C" {
#include <zf/zf.h>
#include <zf/zf_tcp.h>
}

static std::atomic<bool> g_running{true};

// ---------- tiny helpers ----------
static inline uint64_t now_ns() {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

static void pin_to_cpu(int cpu) {
  if (cpu < 0) return;
  cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu, &set);
  pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

// ---------- lock-free SPSC ring ----------
template <typename T, size_t N>
struct SpscRing {
  static_assert((N & (N-1)) == 0, "N must be power of two");
  alignas(64) T buf[N];
  alignas(64) std::atomic<size_t> head{0};
  alignas(64) std::atomic<size_t> tail{0};

  bool push(const T& v) {
    size_t h = head.load(std::memory_order_relaxed);
    size_t t = tail.load(std::memory_order_acquire);
    if (((h + 1) & (N-1)) == (t & (N-1))) return false; // full
    buf[h & (N-1)] = v;
    head.store(h + 1, std::memory_order_release);
    return true;
  }
  bool pop(T& out) {
    size_t t = tail.load(std::memory_order_relaxed);
    size_t h = head.load(std::memory_order_acquire);
    if (t == h) return false; // empty
    out = buf[t & (N-1)];
    tail.store(t + 1, std::memory_order_release);
    return true;
  }
};

struct Message {
  uint32_t len;
  uint64_t rx_done_ns;
  uint64_t send_start_ns;
  uint64_t send_end_ns;
  // Big enough for jumbo; adjust if your relay caps at MTU 1500.
  static constexpr size_t MAX = 9200;
  uint8_t data[MAX];
};

// ---------- CLI (same flags you already use) ----------
struct Args {
  std::string listen_ip = "0.0.0.0";
  uint16_t    listen_port = 9000;
  std::string fwd_ip = "127.0.0.1";
  uint16_t    fwd_port = 9001;
  int rx_cpu = -1;
  int tx_cpu = -1;
  bool enable_latency = false;
  std::string log_path = "";
};

static void usage(const char* argv0) {
  std::cerr <<
    "Usage: " << argv0 << " --listen-ip IP --listen-port P --forward-ip IP --forward-port P\n"
    "       [--rx-cpu N] [--tx-cpu N] [--latency] [--log FILE]\n";
}

static bool parse_args(int argc, char** argv, Args& a) {
  for (int i=1;i<argc;i++) {
    std::string s = argv[i];
    auto nxt = [&](uint16_t off=1)->const char*{ if (i+off>=argc) {usage(argv[0]); exit(2);} return argv[i+off]; };
    if (s=="--listen-ip") a.listen_ip = nxt();
    else if (s=="--listen-port") a.listen_port = (uint16_t)atoi(nxt());
    else if (s=="--forward-ip") a.fwd_ip = nxt();
    else if (s=="--forward-port") a.fwd_port = (uint16_t)atoi(nxt());
    else if (s=="--rx-cpu") a.rx_cpu = atoi(nxt());
    else if (s=="--tx-cpu") a.tx_cpu = atoi(nxt());
    else if (s=="--latency") a.enable_latency = true;
    else if (s=="--log") a.log_path = nxt();
    else { usage(argv[0]); return false; }
  }
  return true;
}

// ---------- TCPDirect wrappers (per-thread stack + muxer) ----------
struct TpdStack {
  zf_attr*  attr  = nullptr;
  zf_stack* stack = nullptr;
  zf_muxer* mux   = nullptr;

  void init() {
    int rc;
    if ((rc = zf_attr_alloc(&attr)) != 0) { fprintf(stderr, "zf_attr_alloc rc=%d\n", rc); exit(1); }
    // attr tuning here if needed (e.g., interface selection)
    if ((rc = zf_stack_alloc(attr, &stack)) != 0) { fprintf(stderr, "zf_stack_alloc rc=%d\n", rc); exit(1); }
    if ((rc = zf_muxer_alloc(stack, &mux)) != 0) { fprintf(stderr, "zf_muxer_alloc rc=%d\n", rc); exit(1); }
  }
  ~TpdStack() {
    if (mux) zf_muxer_free(mux);
    if (stack) zf_stack_free(stack);
    if (attr) zf_attr_free(attr);
  }
};

static sockaddr_in mk_sockaddr(const std::string& ip, uint16_t port) {
  sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port);
  if (inet_pton(AF_INET, ip.c_str(), &a.sin_addr) != 1) {
    fprintf(stderr, "bad ip: %s\n", ip.c_str()); exit(2);
  }
  return a;
}

// RX endpoint = accepted inbound
struct RxEndpoint {
  zftl*   listener = nullptr;
  zft*    in       = nullptr;
};

// TX endpoint = connected outbound
struct TxEndpoint {
  zft_handle* h = nullptr; // handle until connected
  zft*        z = nullptr; // becomes valid after connect completes
};

// ---------- threads ----------
struct Shared {
  SpscRing<Message, 1<<14> ring; // 16K slots
  std::atomic<bool> have_peer{false};
  std::atomic<bool> tx_ready{false};
};

static void rx_thread(const Args a, Shared* sh) {
  pin_to_cpu(a.rx_cpu);

  TpdStack t;
  t.init();

  // Listen inbound
  int rc;
  RxEndpoint ep{};
  sockaddr_in laddr = mk_sockaddr(a.listen_ip, a.listen_port);
  if ((rc = zftl_listen(t.stack, reinterpret_cast<const struct sockaddr*>(&laddr),
                        sizeof(laddr), 128, &ep.listener)) != 0) {
    fprintf(stderr, "zftl_listen rc=%d\n", rc); return;
  }
  zf_waitable* lw = zftl_to_waitable(ep.listener);
  zf_muxer_set_interest(t.mux, lw, EPOLLIN);

  // Wait for inbound connection
  while (g_running && ep.in == nullptr) {
    zf_muxer_wait(t.mux, nullptr, 0, 100);
    if (zf_muxer_is_set(t.mux, lw, EPOLLIN)) {
      if ((rc = zftl_accept(ep.listener, &ep.in)) != 0) {
        if (rc == -EAGAIN) continue;
        fprintf(stderr, "zftl_accept rc=%d\n", rc); return;
      }
    }
  }
  if (!g_running) return;

  zf_waitable* iw = zft_to_waitable(ep.in);
  zf_muxer_set_interest(t.mux, iw, EPOLLIN | EPOLLERR);
  sh->have_peer.store(true, std::memory_order_release);

  // Receive loop
  while (g_running) {
    // Wait for data
    int ne = zf_muxer_wait(t.mux, nullptr, 0, 1000);
    (void)ne;

    // Read
    for (;;) {
      ssize_t n;
#if defined(ZF_HAS_ZFT_RECV)
      n = zft_recv(ep.in, nullptr, 0, 0); // probe
      if (n == -EAGAIN) break;
      if (n <= 0) { g_running=false; break; }
      Message m{};
      if ((size_t)n > Message::MAX) { fprintf(stderr, "oversize %zd\n", n); g_running=false; break; }
      // real read
      n = zft_recv(ep.in, m.data, Message::MAX, 0);
      if (n <= 0) { g_running=false; break; }
      m.len = (uint32_t)n;
      if (a.enable_latency) m.rx_done_ns = now_ns();
      while (!sh->ring.push(m) && g_running) { /* backoff */ }
#else
      // zc path (copy into our buffer)
      struct iovec iov[32]; int iovcnt = 32;
      n = zft_recv_zc(ep.in, iov, &iovcnt, 0);
      if (n == -EAGAIN) break;
      if (n <= 0) { g_running=false; break; }
      Message m{};
      size_t need = (size_t)n;
      if (need > Message::MAX) { fprintf(stderr, "oversize %zu\n", need); g_running=false; break; }
      size_t off=0;
      for (int i=0;i<iovcnt && off<need;i++) {
        size_t cp = std::min(need - off, (size_t)iov[i].iov_len);
        std::memcpy(m.data + off, iov[i].iov_base, cp);
        off += cp;
      }
      zft_zc_recv_done(ep.in, iov, iovcnt);
      m.len = (uint32_t)need;
      if (a.enable_latency) m.rx_done_ns = now_ns();
      while (!sh->ring.push(m) && g_running) { /* backoff */ }
#endif
    }
  }
}

static void tx_thread(const Args a, Shared* sh) {
  pin_to_cpu(a.tx_cpu);

  TpdStack t;
  t.init();

  // Outbound connect
  int rc;
  TxEndpoint ep{};
  if ((rc = zft_alloc(t.stack, t.attr, &ep.h)) != 0) {
    fprintf(stderr, "zft_alloc rc=%d\n", rc); return;
  }
  sockaddr_in raddr = mk_sockaddr(a.fwd_ip, a.fwd_port);
  if ((rc = zft_connect(ep.h, reinterpret_cast<const struct sockaddr*>(&raddr), sizeof(raddr))) != 0) {
    fprintf(stderr, "zft_connect rc=%d\n", rc); return;
  }
  // Wait for connect-complete
  zf_waitable* hw = zft_handle_to_waitable(ep.h);
  zf_muxer_set_interest(t.mux, hw, EPOLLOUT|EPOLLERR);
  for (;;) {
    zf_muxer_wait(t.mux, nullptr, 0, 100);
    if (zf_muxer_is_set(t.mux, hw, EPOLLOUT)) {
      if ((rc = zft_handle_to_zft(ep.h, &ep.z)) != 0) {
        if (rc == -EAGAIN) continue;
        fprintf(stderr, "zft_handle_to_zft rc=%d\n", rc); return;
      }
      break;
    }
  }
  zf_waitable* zw = zft_to_waitable(ep.z);
  zf_muxer_set_interest(t.mux, zw, EPOLLOUT|EPOLLERR);
  sh->tx_ready.store(true, std::memory_order_release);

  // Optional logging
  std::ofstream log;
  if (!a.log_path.empty()) log.open(a.log_path, std::ios::out | std::ios::app);

  // Send loop
  Message m;
  while (g_running) {
    if (!sh->ring.pop(m)) { zf_muxer_wait(t.mux, nullptr, 0, 100); continue; }

    size_t off = 0;
    while (off < m.len && g_running) {
      if (a.enable_latency) m.send_start_ns = now_ns();
      ssize_t s = zft_send(ep.z, m.data + off, m.len - off, 0);
      if (s == -EAGAIN) {
        zf_muxer_wait(t.mux, nullptr, 0, 100);
        continue;
      }
      if (s <= 0) { g_running=false; break; }
      off += (size_t)s;
      if (a.enable_latency) {
        m.send_end_ns = now_ns();
        if (log.good()) {
          // CSV: recv_done_ns, send_start_ns, send_end_ns, total_ns
          uint64_t total = (m.send_end_ns >= m.rx_done_ns) ? (m.send_end_ns - m.rx_done_ns) : 0;
          log << m.rx_done_ns << "," << m.send_start_ns << "," << m.send_end_ns << "," << total << "\n";
        }
      }
    }
  }
}

// ---------- main ----------
int main(int argc, char** argv) {
  Args a;
  if (!parse_args(argc, argv)) return 2;

  // Ctrl-C
  std::signal(SIGINT,  [](int){ g_running=false; });
  std::signal(SIGTERM, [](int){ g_running=false; });

  // TCPDirect global init (once in the process)
  int rc;
  if ((rc = zf_init()) != 0) {
    std::fprintf(stderr, "zf_init rc=%d\n", rc);
    return 1;
  }

  Shared sh;

  std::thread rxt(rx_thread, a, &sh);
  std::thread txt(tx_thread, a, &sh);

  rxt.join();
  txt.join();
  return 0;
}
