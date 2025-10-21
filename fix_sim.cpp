#include <iostream>
#include <thread>
#include <chrono>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>  // for TCP_NODELAY
#include <arpa/inet.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <dest_ip> <dest_port> <num_messages>\n";
        return 1;
    }

    const char* dest_ip = argv[1];
    int dest_port = std::stoi(argv[2]);
    int num_messages = std::stoi(argv[3]);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    // Disable Nagle's algorithm
    int flag = 1;
    if (setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int)) < 0) {
        perror("setsockopt TCP_NODELAY");
    } else {
        std::cout << "[sender] TCP_NODELAY enabled\n";
    }

    sockaddr_in dest_addr{};
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(dest_port);
    inet_pton(AF_INET, dest_ip, &dest_addr.sin_addr);

    if (connect(sock, (sockaddr*)&dest_addr, sizeof(dest_addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    std::cout << "[sender] Connected to " << dest_ip << ":" << dest_port << "\n";

    for (int i = 0; i < num_messages; ++i) {
        std::string msg = "FIX" + std::to_string(i) + "|55=TEST|10=000|\n";
        ssize_t sent = send(sock, msg.c_str(), msg.size(), 0);
        if (sent < 0) {
            perror("send");
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); // optional pacing
    }

    close(sock);
    std::cout << "[sender] Finished\n";
    return 0;
}
