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
  class TcpSession {
  public:
    TcpSession();
    TcpSession(Memcached*, NetworkManager::TcpPcb);
  private:
    void Receive(NetworkManager::TcpPcb &t, std::unique_ptr<IOBuf> b);
    Memcached *mcd_;
    NetworkManager::TcpPcb tcp_;
    std::unique_ptr<IOBuf> queued_bufs_;
  }; 

  int set_count_;
  int get_count_;
  int other_count_;
  uint16_t listening_port_;
  NetworkManager::TcpPcb tcp_;
  std::unordered_map<std::string, std::unique_ptr<IOBuf> > map_;
  static const char *com2str(uint8_t);

  void Set(std::unique_ptr<IOBuf>, uint32_t);
  std::unique_ptr<IOBuf> Get(std::unique_ptr<IOBuf>, uint32_t);
  void Quit();
  void Flush();
// these are binary specific.. for now
  void Nop(protocol_binary_request_header &);
  void Unimplemented(protocol_binary_request_header &);
};
} // namespace ebbrt

#endif // MEMCACHED_H
