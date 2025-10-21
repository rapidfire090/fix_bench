#include <iostream>
#include <thread>
#include <array>
#include <vector>
#include <cstring>
#include <atomic>
#include <chrono>
#include <fstream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>

// Onload Extensions
#include <onload/extensions.h>

struct Message {
    std::array<char, 1024> data;
    size_t length;
    uint64_t recv_end_ns;
};

struct LogEntry {
    uint64_t timestamp_ns;
    uint64_t latency_ns;
    uint64_t send_latency_ns;
    uint64_t total_latency_ns;
    std::array<char, 32> clordid;
};

template <typename T, size_t Capacity>
class SPSCQueue {
private:
    std::array<T, Capacity> buffer;
    std::atomic<size_t> head{0};
    std::atomic<size_t> tail{0};

public:
    bool push(const T& item) {
        size_t h = head.load(std::memory_order_relaxed);
        size_t next = (h + 1) % Capacity;
        if (next == tail.load(std::memory_order_acquire)) return false;
        buffer[h] = item;
        head.store(next, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t t = tail.load(std::memory_order_relaxed);
        if (t == head.load(std::memory_order_acquire)) return false;
        item = buffer[t];
        tail.store((t + 1) % Capacity, std::memory_order_release);
        return true;
    }
};

SPSCQueue<Message, 256> queue;
SPSCQueue<LogEntry, 4096> log_queue;

std::atomic<bool> enable_latency{false};
std::atomic<int> debug_level{0};
std::string log_file_path;
int log_flush_interval_ms = 50;

std::string extract_fix_tag11(const char* data, size_t len) {
    std::string msg(data, len);
    size_t pos = msg.find("11=");
    if (pos == std::string::npos) return "";
    size_t end = msg.find('\x01', pos);
    if (end == std::string::npos) return msg.substr(pos + 3);
    return msg.substr(pos + 3, end - pos - 3);
}

void log_writer_thread(const std::string& file_path, int flush_interval_ms) {
    sched_param param{};
    param.sched_priority = 0;
    pthread_setschedparam(pthread_self(), SCHED_OTHER, &param);

    std::ofstream out(file_path, std::ios::out | std::ios::app);
    if (!out) {
        std::cerr << "Failed to open log file: " << file_path << std::endl;
        return;
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(flush_interval_ms));
        LogEntry entry;
        while (log_queue.pop(entry)) {
            out << entry.timestamp_ns << ","
                << entry.latency_ns << ","
                << entry.send_latency_ns << ","
                << entry.total_latency_ns << ","
                << entry.clordid.data() << "\n";
        }
        out.flush();
    }
}

// RX: accelerate this thread, move accepted socket into this stack
void recv_thread(int client_sock) {
    onload_set_stackname(ONLOAD_THIS_THREAD, ONLOAD_SCOPE_THREAD, "rx_stack");
    onload_thread_set_spin(ONLOAD_SPIN_ALL, 1);  // optional

    // Move accepted socket (created in main) into this thread's Onload stack
    if (onload_move_fd(client_sock) < 0) {
        // If move fails (kernel socket / not onload-capable), continue anyway
        // perror("[recv] onload_move_fd");
    }

    std::cout << "[recv] Thread started" << std::endl;
    while (true) {
        Message msg;
        ssize_t len = recv(client_sock, msg.data.data(), msg.data.size(), 0);

        msg.recv_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();

        if (len <= 0) {
            if (len == 0)
                std::cout << "[recv] Client closed connection" << std::endl;
            else
                perror("[recv] recv error");
            break;
        }

        msg.length = static_cast<size_t>(len);
        int spin = 0;
        while (!queue.push(msg)) {
            if (++spin > 1000) spin = 0;
        }
    }

    close(client_sock);
    std::cout << "[recv] Closed client socket" << std::endl;
}

// TX: accelerate this thread; create forward socket in this stack
void send_thread(const char* forward_ip, int forward_port) {
    onload_set_stackname(ONLOAD_THIS_THREAD, ONLOAD_SCOPE_THREAD, "tx_stack");
    onload_thread_set_spin(ONLOAD_SPIN_ALL, 1);  // optional

    std::cout << "[send] Connecting to " << forward_ip << ":" << forward_port << std::endl;

    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (forward_sock < 0) {
        perror("[send] socket");
        return;
    }

    int flag = 1;
    setsockopt(forward_sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    sockaddr_in forward_addr{};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(forward_port);
    inet_pton(AF_INET, forward_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("[send] connect");
        close(forward_sock);
        return;
    }

    std::cout << "[send] Connected" << std::endl;

    Message msg;
    while (true) {
        int spin = 0;
        while (!queue.pop(msg)) {
            if (++spin > 1000) spin = 0;
        }

        uint64_t send_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();

        ssize_t sent = send(forward_sock, msg.data.data(), msg.length, 0);

        uint64_t send_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();

        if (sent < 0) {
            perror("[send] send");
            break;
        }

        if (enable_latency && debug_level == 2) {
            uint64_t latency = send_start_ns - msg.recv_end_ns;
            uint64_t send_latency = send_end_ns - send_start_ns;
            uint64_t total = latency + send_latency;

            std::string clordid_str = extract_fix_tag11(msg.data.data(), msg.length);
            LogEntry entry{msg.recv_end_ns, latency, send_latency, total};
            std::strncpy(entry.clordid.data(), clordid_str.c_str(), entry.clordid.size() - 1);
            log_queue.push(entry);
        }
    }

    close(forward_sock);
    std::cout << "[send] Forward socket closed" << std::endl;
}

// Sleeper: default (no Onload calls), pinned to its CPU
void sleeper_thread() {
    std::cout << "[sleeper] Thread started (sleeping indefinitely)" << std::endl;
    while (true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
    }
}

int main(int argc, char* argv[]) {
    if (argc < 8) {
        std::cerr << "Usage: " << argv[0]
                  << " <listen_ip> <listen_port> <forward_ip> <forward_port> <rx_cpu> <tx_cpu> <sleep_cpu> "
                  << "[--measure-latency <log_file> <flush_interval_ms> [--debug-level=2]]" << std::endl;
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);
    const char* forward_ip = argv[3];
    int forward_port = std::stoi(argv[4]);
    int rx_cpu = std::stoi(argv[5]);
    int tx_cpu = std::stoi(argv[6]);
    int sleep_cpu = std::stoi(argv[7]);

    for (int i = 8; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--measure-latency" && i + 2 < argc) {
            enable_latency = true;
            log_file_path = argv[++i];
            log_flush_interval_ms = std::stoi(argv[++i]);
        } else if (arg.rfind("--debug-level=", 0) == 0) {
            debug_level = std::stoi(arg.substr(14));
        }
    }

    if (enable_latency && log_file_path.size() > 0) {
        std::thread logger(log_writer_thread, log_file_path, log_flush_interval_ms);
        logger.detach();
    }

    // Start sleeper and pin it
    std::thread sleeper(sleeper_thread);
    {
        cpu_set_t set_sl;
        CPU_ZERO(&set_sl);
        CPU_SET(sleep_cpu, &set_sl);
        pthread_setaffinity_np(sleeper.native_handle(), sizeof(cpu_set_t), &set_sl);
    }
    sleeper.detach();

    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        perror("[main] socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in listen_addr{};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(listen_port);
    inet_pton(AF_INET, listen_ip, &listen_addr.sin_addr);

    if (bind(listen_sock, (sockaddr*)&listen_addr, sizeof(listen_addr)) < 0) {
        perror("[main] bind");
        return 1;
    }

    if (listen(listen_sock, 10) < 0) {
        perror("[main] listen");
        return 1;
    }

    std::cout << "[main] Listening on " << listen_ip << ":" << listen_port << std::endl;

    while (true) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        if (client_sock >= 0) {
            std::cout << "[main] Accepted connection" << std::endl;

            // RX thread will move this fd into its own stack
            std::thread rx(recv_thread, client_sock);
            cpu_set_t set_rx;
            CPU_ZERO(&set_rx);
            CPU_SET(rx_cpu, &set_rx);
            pthread_setaffinity_np(rx.native_handle(), sizeof(cpu_set_t), &set_rx);
            rx.detach();

            // TX thread creates its own accelerated socket
            std::thread tx(send_thread, forward_ip, forward_port);
            cpu_set_t set_tx;
            CPU_ZERO(&set_tx);
            CPU_SET(tx_cpu, &set_tx);
            pthread_setaffinity_np(tx.native_handle(), sizeof(cpu_set_t), &set_tx);
            tx.detach();
        }
    }

    close(listen_sock);
    return 0;
}
