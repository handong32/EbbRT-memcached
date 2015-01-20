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
#include "tcp_handler.hpp"

namespace ebbrt {
class Memcached : public StaticSharedEbb<Memcached>, public CacheAligned {
public:
  Memcached();
  void Start(uint16_t port);

private:
  class GetResponse {
  public:
    GetResponse();
    GetResponse(std::unique_ptr<IOBuf>);
    std::unique_ptr<IOBuf> Ascii();
    std::unique_ptr<IOBuf> Binary();
  private:
    bool binary_;
    std::unique_ptr<IOBuf> request_;
    std::unique_ptr<IOBuf> ascii_response_;
    std::unique_ptr<IOBuf> binary_response_;
  }; 
  
  class TcpSession : public TcpHandler {
  public:
    TcpSession(Memcached *mcd, ebbrt::NetworkManager::TcpPcb pcb)
      : TcpHandler(std::move(pcb)), mcd_(mcd) {}
    void Close(){}
    void Abort(){}
    void Receive(std::unique_ptr<MutIOBuf> b);

  private:
    std::unique_ptr<ebbrt::MutIOBuf> buf_;
    ebbrt::NetworkManager::TcpPcb pcb_;
    Memcached *mcd_;
  };

  
  std::unique_ptr<IOBuf> ProcessAscii(std::unique_ptr<IOBuf>, std::string);
  std::unique_ptr<IOBuf> ProcessBinary(std::unique_ptr<IOBuf>, protocol_binary_response_header*);
  static const char *com2str(uint8_t);
  GetResponse* Get(std::unique_ptr<IOBuf>, std::string);
  void Set(std::unique_ptr<IOBuf>, std::string);
  void Quit();
  void Flush();
  NetworkManager::ListeningTcpPcb listening_pcb_;
  std::unordered_map<std::string, GetResponse> map_;
  //fixme: below two are binary specific.. for now
  void Nop(protocol_binary_request_header &);
  void Unimplemented(protocol_binary_request_header &);
};
} //namespace ebbrt

#endif // MEMCACHED_H
