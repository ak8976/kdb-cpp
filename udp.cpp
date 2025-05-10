#include <iostream>
#include <unordered_map>
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/eventfd.h>
#include "poll.h"
#include "k.h"

static int listening_socket{};
static bool loop_running{false};
static std::unordered_map<int, sockaddr_in> sock_to_addr;
static int event_fd;
static constexpr char ASYNC_BYTE = 0x00;
static constexpr char SYNC_BYTE = 0x01;
static constexpr char RCV_BYTE = 0xFF;
constexpr size_t MSG_MAX_SIZE = 65506;
static pollfd fds[1];
static iovec iov[2];
static msghdr mhdr{};
static char tag_byte;
static char recv_buffer[MSG_MAX_SIZE];

sockaddr_in get_socket_info(const char* host, I port) {
  addrinfo hints{}, *res = nullptr;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  char hostname[256];
  if (strcmp(host, "localhost") == 0) {
    gethostname(hostname, sizeof(hostname));
    host = hostname;
  }
  sockaddr_in dest{};
  if (getaddrinfo(host, nullptr, &hints, &res) != 0 || !res) return dest;
  sockaddr_in* resolved = (sockaddr_in*)res->ai_addr;
  dest.sin_family = AF_INET;
  dest.sin_port = htons(port);
  dest.sin_addr = resolved->sin_addr;
  freeaddrinfo(res);
  return dest;
}
void set_iov(void* bytes, const size_t bytes_len, sockaddr_in* addr) {
  iov[0].iov_base = &tag_byte;
  iov[0].iov_len = 1;
  iov[1].iov_base = bytes;
  iov[1].iov_len = bytes_len;
  mhdr.msg_name = addr;
  mhdr.msg_namelen = sizeof(*addr);
  mhdr.msg_iov = iov;
  mhdr.msg_iovlen = 2;
}
K udp_send_base(int sock, K msg, char mode) {
  if (sock_to_addr.find(sock) == sock_to_addr.end())
    return krr((S) "unknown socket");
  tag_byte = mode;
  K serialized = ee(b9(2, msg));
  if (!serialized || serialized->n + 1 > MSG_MAX_SIZE) {
    return krr((S) "badmsg");
  }
  sockaddr_in dest = sock_to_addr[sock];
  set_iov(kG(serialized), serialized->n, &dest);
  ssize_t n = sendmsg(sock, &mhdr, 0);
  r0(serialized);
  if (n < 0) return krr((S) "sendmsg");
  if (tag_byte == SYNC_BYTE) {
    sockaddr_in sender{};
    set_iov(recv_buffer, MSG_MAX_SIZE, &sender);
    n = recvmsg(sock, &mhdr, 0);
    if (n < 0) return krr((S) "recvmsg");
    serialized = ktn(KG, n - 1);
    memcpy(kG(serialized), recv_buffer, n - 1);
    K rcv_data = ee(d9(serialized));
    r0(serialized);
    if (rcv_data->t == -128) {
      S err = rcv_data->s;
      r0(rcv_data);
      return krr(err);
    }
    return rcv_data;
  }
  return (K)0;
}
void udp_dequeue(int sock) {
  sockaddr_in sender{};
  K serialized = ktn(KG, MSG_MAX_SIZE);
  set_iov(kG(serialized), MSG_MAX_SIZE, &sender);
  ssize_t n = recvmsg(sock, &mhdr, 0);
  if (n < 0) {
    r0(serialized);
    return;
  }
  serialized->n = n - 1;
  K msg = ee(d9(serialized));
  r0(serialized);
  K ret_data = k(0, (S) "value", r1(msg), (K)0);
  r0(msg);
  if (ret_data->t == -128) {
    std::cout << ret_data->s << std::endl;
    r0(ret_data);
    return;
  }
  if (tag_byte == SYNC_BYTE) {
    sock_to_addr[sock] = sender;
    udp_send_base(sock, ret_data, RCV_BYTE);
  }
  r0(ret_data);
}
void udp_listen() {
  K port = k(0, (S) "\\p", (K)0);
  int p = port->i;
  int sock = socket(AF_INET, SOCK_DGRAM, 0);
  if (sock < 0) return;
  sockaddr_in addr = get_socket_info("localhost", p);
  if (bind(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
    close(sock);
    return;
  }
  int flags = fcntl(sock, F_GETFL, 0);
  fcntl(sock, F_SETFL, flags | O_NONBLOCK);
  listening_socket = sock;
}
K udp_read_msg(int d) {
  int ready = poll(fds, 1, 0);
  if (ready && (fds[0].revents & POLLIN)) udp_dequeue(listening_socket);
  return (K)0;
}

// Functions exposed to q
extern "C" K udp_send(K sock, K msg) {
  if (sock->t != -KI) return krr((S) "type");
  return udp_send_base(sock->i, msg, SYNC_BYTE);
}
extern "C" K udp_send_async(K sock, K msg) {
  if (sock->t != -KI) return krr((S) "type");
  return udp_send_base(sock->i, msg, ASYNC_BYTE);
}
extern "C" K udp_register(K host, K port) {
  if (host->t != -KS || port->t != -KI) return krr((S) "type");
  int sock = socket(AF_INET, SOCK_DGRAM, 0);
  timeval tv{};
  tv.tv_sec = 30;
  if (sock < 0) return krr((S) "socket creation failed");
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  sockaddr_in dest = get_socket_info(host->s, port->i);
  if (dest.sin_family == 0) return krr((S) "failed to resolve host");
  sock_to_addr[sock] = dest;
  std::cout << "Registered host=" << host->s << ", port=" << port->i << ":"
            << sock << std::endl;
  return ki(sock);
}
extern "C" K udp_deregister(K sock) {
  if (sock->t != -KI) return krr((S) "type");
  bool removed = sock_to_addr.erase(sock->i) > 0;
  if (removed) {
    close(sock->i);
    std::cout << "Deregistered socket " << sock->i << std::endl;
  }
  return kb(removed);
}
extern "C" K udp_event_loop_start() {
  if (loop_running) return krr((S) "event loop already running");
  loop_running = true;
  if (!listening_socket) udp_listen();
  if (listening_socket) {
    fds[0].fd = listening_socket;
    fds[0].events = POLLIN;
    event_fd = eventfd(1, EFD_NONBLOCK);
    if (event_fd != -1) {
      std::cout << "UDP ipc enabled" << std::endl;
      sd1(-event_fd, udp_read_msg);
    };
  }
  return (K)0;
}
extern "C" K udp_event_loop_stop() {
  if (!loop_running) return krr((S) "event loop already stopped");
  loop_running = false;
  close(listening_socket);
  listening_socket = 0;
  sd0(event_fd);
  std::cout << "UDP ipc disabled" << std::endl;
  return (K)0;
}
