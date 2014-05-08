//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
#include <cstdlib>
#include <sstream>
#include <ebbrt/Debug.h>
#include <ebbrt/Messenger.h>
#include <ebbrt/Trace.h>
#include "Memcached.h"

ebbrt::Memcached::Memcached() : set_count_(0), get_count_(0), other_count_(0) {}

const char *ebbrt::Memcached::com2str(uint8_t cmd) {
  static const char *const text[] = {
    "GET",       "SET",      "ADD",     "REPLACE",    "DELETE",     "INCREMENT",
    "DECREMENT", "QUIT",     "FLUSH",   "GETQ",       "NOOP",       "VERSION",
    "GETK",      "GETKQ",    "APPEND",  "PREPEND",    "STAT",       "SETQ",
    "ADDQ",      "REPLACEQ", "DELETEQ", "INCREMENTQ", "DECREMENTQ", "QUITQ",
    "FLUSHQ",    "APPENDQ",  "PREPENDQ"
  };

  if (cmd <= PROTOCOL_BINARY_CMD_PREPENDQ) {
    return text[cmd];
  }
  return nullptr;
}


std::unique_ptr<ebbrt::IOBuf>
ebbrt::Memcached::Get(std::unique_ptr<IOBuf> b, uint32_t keylen) {
  auto key = std::string(reinterpret_cast<const char *>(b->Data()), keylen);
  auto query = map_.find(key);
  if (query == map_.end()) {
    // cache miss
    return nullptr;
  } else {
    // cache hit
    return query->second->Clone();
  }
}

//ebbrt::Memcached::Get(NetworkManager::TcpPcb *pcb,
//                           protocol_binary_request_header &h, std::unique_ptr<IOBuf> b) {
//  // pull key from the request header
//  auto p = b->GetDataPointer();
//  p.Advance(sizeof(protocol_binary_request_get));
//  auto keylenp = reinterpret_cast<char *>(&h.request.keylen);
//  //FIXME ntohl:
//  auto keylen = (keylenp[1] << 0) | (keylenp[0] << 8); // machine order
//  //fixme:
//  auto key = std::string(reinterpret_cast<const char *>(p.Data()), keylen);
//  // contruct response buffer
//  uint16_t status = 0;
//  uint8_t extlen = sizeof(uint32_t);
//  uint32_t bodylen = extlen;
//  auto buf = IOBuf::Create(sizeof(protocol_binary_response_header)+extlen, true);
//  auto res = reinterpret_cast<protocol_binary_response_get *>(buf->WritableData());
//  res->message.header.response.magic = PROTOCOL_BINARY_RES;
//  res->message.header.response.extlen = extlen;
//  // check cache for data
//  auto query = map_.find(key);
//  if (query == map_.end()) {
//    // cache miss
//    return nullptr;
//  } else {
//    // cache hit
//    auto kv = query->second->Clone();
//    if (h.request.opcode != PROTOCOL_BINARY_CMD_GETK) {
//      // Advance or Retreat to cut off key
//      res->message.header.response.opcode = PROTOCOL_BINARY_CMD_GET;
//      kv->Advance(keylen);
//    }else
//    {
//      res->message.header.response.opcode = PROTOCOL_BINARY_CMD_GETK;
//      res->message.header.response.keylen = h.request.keylen;
//    }
//    bodylen += kv->ComputeChainDataLength();
//    buf->AppendChain(std::move(kv));
//  }
//  res->message.header.response.status = status;
//  res->message.header.response.bodylen = htonl(bodylen);
//  pcb->Send(std::move(buf));
//}

void ebbrt::Memcached::Set(std::unique_ptr<IOBuf> b, uint32_t keylen){
  auto key = std::string(reinterpret_cast<const char *>(b->Data()), keylen);
  map_[key] = std::move(b);
}

//void ebbrt::Memcached::Set(NetworkManager::TcpPcb *pcb,
//                           protocol_binary_request_header &h, std::unique_ptr<IOBuf> b) {
//  auto p = b->GetDataPointer();
//  /// manually correct endianness where necessary
//  auto keylenp = reinterpret_cast<char *>(&h.request.keylen);
//  ///FIXME:
//  auto keylen = (keylenp[1] << 0) | (keylenp[0] << 8);
//  auto keyoffset = sizeof(protocol_binary_request_header) + h.request.extlen;
//  p.Advance(sizeof(protocol_binary_request_header) + h.request.extlen);
//  auto keyptr = reinterpret_cast<const char *>(p.Data());
//  p.Advance(keylen);
//  auto key = std::string(keyptr, keylen);
//  // point buffer to begining of value
//  b->Advance(keyoffset);
//  map_[key] = std::move(b);
//  if (h.request.opcode == PROTOCOL_BINARY_CMD_SET) {
//    // construct reply message
//    auto buf = IOBuf::Create(sizeof(protocol_binary_response_header), true);
//    auto res = reinterpret_cast<protocol_binary_response_header *>(buf->WritableData());
//    res->response.magic = PROTOCOL_BINARY_RES;
//    res->response.opcode = PROTOCOL_BINARY_CMD_SET;
//    pcb->Send(std::move(buf));
//  }
//}

void ebbrt::Memcached::Quit() {
  // winners never quit!
}

void ebbrt::Memcached::Flush() {
  map_.clear();
}

void ebbrt::Memcached::Nop(protocol_binary_request_header &h) {
  const char *cmd = com2str(h.request.opcode);
  kprintf("%s CMD IS NOP\n", cmd);
}

void ebbrt::Memcached::Unimplemented(protocol_binary_request_header &h) {
  const char *cmd = com2str(h.request.opcode);
  kbugon(true, "%s CMD IS UNSUPPORTED. ABORTING...\n", cmd);
}

void ebbrt::Memcached::StartListening(uint16_t port) {
  listening_port_ = port;
  tcp_.Bind(listening_port_);
  tcp_.Listen();
  tcp_.Accept([this](NetworkManager::TcpPcb pcb) { new TcpSession(this, std::move(pcb));});
}

void ebbrt::Memcached::TcpSession::Receive(NetworkManager::TcpPcb &t,
                                           std::unique_ptr<IOBuf> b) {

  std::unique_ptr<IOBuf> *buf;
  bool in_queue = false;

  // clean up and exit on empty payload (aka. connection closed)
  if ((b)->Length() == 0) {
    delete this;
    return;
  }

  // restore any previously queue message fragments
  if (queued_bufs_ != nullptr) {
    queued_bufs_->Prev()->AppendChain(std::move(b));
    buf = &queued_bufs_;
    in_queue = true;
  } else {
    buf = &b;
  }

  auto bp = ((*buf))->Data();
  auto chain_len = (*buf)->ComputeChainDataLength();
  auto bdata = (*buf)->GetDataPointer();

  // check for binary protocol
  if (bp[0] == PROTOCOL_BINARY_REQ){ 
    auto h = bdata.Get<protocol_binary_request_header>();
    auto message_len =
      sizeof(protocol_binary_request_header) + htonl(h.request.bodylen);
    // confirm we have the full message
    if (chain_len < message_len) {
      if (!in_queue)
        queued_bufs_ = std::move((*buf));
      return;
    } else if (chain_len > message_len) {
      kabort("memcached: chain_len[%d] > message_len[%d]\n", chain_len,
          message_len);
    } else {
      // we have received a full request
      kassert(chain_len == message_len);
      if (in_queue) { queued_bufs_.reset(nullptr); }

      // pull key from the request header
      auto keylen = ntohl(h.request.keylen << 16);

      auto keyoffset = sizeof(protocol_binary_request_header) + h.request.extlen;
      (*buf)->Advance(keyoffset);

      // fill for particular response type
      uint16_t status = 0;
      uint8_t  extlen = 0;
      uint32_t bodylen = 0;
      uint32_t rkeylen = 0;
      std::unique_ptr<IOBuf> kv(nullptr);
      auto keybuf = IOBuf::Create(keylen, true);
      auto keyptr = reinterpret_cast<char *>(keybuf->WritableData());

      switch (h.request.opcode) {
        case PROTOCOL_BINARY_CMD_SET:
          mcd_->Set(std::move((*buf)), keylen);
          break;
        case PROTOCOL_BINARY_CMD_SETQ:
          mcd_->Set(std::move((*buf)), keylen);
          return;
        case PROTOCOL_BINARY_CMD_GET:
          kv = mcd_->Get(std::move((*buf)), keylen);
          extlen = sizeof(uint32_t);
          bodylen = extlen;
          if (kv) {
            kv->Advance(keylen);
            bodylen += kv->ComputeChainDataLength();
          } else {
            status = 1;
          }
          break;
        case PROTOCOL_BINARY_CMD_GETQ:
          kv = mcd_->Get(std::move((*buf)), keylen);
          extlen = sizeof(uint32_t);
          if (kv) {
            kv->Advance(keylen);
            bodylen += kv->ComputeChainDataLength();
          } else {
            return;
          }
          break;
        case PROTOCOL_BINARY_CMD_GETK:
          // store key before we relinquish buffer
          rkeylen = keylen;
          std::memcpy(keyptr, reinterpret_cast<const char *>((*buf)->Data()), keylen);
          extlen = sizeof(uint32_t);
          bodylen = extlen;
          kv = mcd_->Get(std::move((*buf)), keylen);
          if (kv) {
            bodylen += kv->ComputeChainDataLength();
          } else {
            // return miss + key
            status = 1;
            bodylen += keybuf->Length();
            kv = std::move(keybuf);
          }
          break;
        case PROTOCOL_BINARY_CMD_GETKQ:
          mcd_->Get(std::move((*buf)), keylen);
          break;
        case PROTOCOL_BINARY_CMD_NOOP:
        case PROTOCOL_BINARY_CMD_VERSION:
          mcd_->Nop(h);
          return;
        case PROTOCOL_BINARY_CMD_QUIT:
          mcd_->Quit();
          break;
        case PROTOCOL_BINARY_CMD_QUITQ:
          mcd_->Quit();
          return;
        case PROTOCOL_BINARY_CMD_FLUSH:
          mcd_->Flush();
          break;
        case PROTOCOL_BINARY_CMD_FLUSHQ:
          mcd_->Flush();
          return;
        default:
          mcd_->Unimplemented(h);
          return;
      }

      // send response 
      auto rbuf = IOBuf::Create(sizeof(protocol_binary_response_header) + extlen, true);
      if (kv) {
        rbuf->AppendChain(std::move(kv));
      }
      auto res = reinterpret_cast<protocol_binary_response_header *>(
          rbuf->WritableData());
      res->response.magic = PROTOCOL_BINARY_RES;
      res->response.status = status;
      res->response.opcode = h.request.opcode;
      res->response.extlen = extlen;
      res->response.keylen = (htonl(rkeylen) >> 16);
      res->response.bodylen = htonl(bodylen);
      t.Send(std::move(rbuf));
    }
  } // end binary processing 
  //TODO: ascii
}

ebbrt::Memcached::TcpSession::TcpSession(Memcached *mcd,
                                         NetworkManager::TcpPcb pcb) {
  mcd_ = mcd;
  tcp_ = std::move(pcb);
  queued_bufs_ = nullptr;
  tcp_.DisableNagle();
  tcp_.Receive([this](NetworkManager::TcpPcb &t,
                      std::unique_ptr<IOBuf> b) { Receive(t, std::move(b)); });
}
