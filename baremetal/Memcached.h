//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
#ifndef MEMCACHED_H
#define MEMCACHED_H

#include <memory>
#include <ebbrt/CacheAligned.h>
#include <ebbrt/Net.h>
#include <ebbrt/SpinLock.h>
#include <ebbrt/StaticSharedEbb.h>
#include "protocol_binary.h"

namespace ebbrt {
class Memcached : public StaticSharedEbb<Memcached>, public CacheAligned {

public:
  Memcached();
  void StartListening(uint16_t port);

private:
  int set_count_;
  int get_count_;
  int other_count_;
  uint16_t port_;
  NetworkManager::TcpPcb tcp_;
  std::unordered_map<std::string, std::unique_ptr<IOBuf>> map_;
  static const char *com2str(uint8_t);

  void Preexecute(NetworkManager::TcpPcb *, protocol_binary_request_header &,
                  std::unique_ptr<IOBuf>);
  void Unimplemented(protocol_binary_request_header &);
  void Set(NetworkManager::TcpPcb *, protocol_binary_request_header &, std::unique_ptr<IOBuf>);
  void Get(NetworkManager::TcpPcb *, protocol_binary_request_header &, std::unique_ptr<IOBuf>);
  void Quit(NetworkManager::TcpPcb *, protocol_binary_request_header &);
  void Flush(NetworkManager::TcpPcb *, protocol_binary_request_header &);
  void Nop(protocol_binary_request_header &);
};
} // namespace ebbrt

#endif // MEMCACHED_H
