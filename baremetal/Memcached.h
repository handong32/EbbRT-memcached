//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
#ifndef MEMCACHED_H
#define MEMCACHED_H

#include <memory>
#include <ebbrt/CacheAligned.h>
//#include <ebbrt/Future.h>
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
  uint16_t port_;
  NetworkManager::TcpPcb tcp_;
  std::unordered_map<std::string, std::shared_ptr<Buffer>> map_;
  static const char* com2str(uint8_t);

  void Preexecute(NetworkManager::TcpPcb*, protocol_binary_request_header&,
                  Buffer);
  void SetCmd(NetworkManager::TcpPcb*, protocol_binary_request_header&, Buffer);
  void GetCmd(NetworkManager::TcpPcb*, protocol_binary_request_header&, Buffer);
  void Unimplemented(NetworkManager::TcpPcb*, protocol_binary_request_header&);
};
}  // namespace ebbrt

#endif  // MEMCACHED_H
