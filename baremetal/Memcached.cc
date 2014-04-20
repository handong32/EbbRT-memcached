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

void ebbrt::Memcached::Preexecute(NetworkManager::TcpPcb *pcb,
                                  protocol_binary_request_header &h, std::unique_ptr<IOBuf> b) {
  switch (h.request.opcode) {
  case PROTOCOL_BINARY_CMD_SET:
  case PROTOCOL_BINARY_CMD_SETQ:
    set_count_++;
    Set(pcb, h, std::move(b));
    break;
  case PROTOCOL_BINARY_CMD_GET:
  case PROTOCOL_BINARY_CMD_GETQ:
  case PROTOCOL_BINARY_CMD_GETK:
  case PROTOCOL_BINARY_CMD_GETKQ:
    get_count_++;
    Get(pcb, h, std::move(b));
    break;
  case PROTOCOL_BINARY_CMD_NOOP:
  case PROTOCOL_BINARY_CMD_VERSION:
    other_count_++;
    Nop(h);
    break;
  case PROTOCOL_BINARY_CMD_QUIT:
  case PROTOCOL_BINARY_CMD_QUITQ:
    other_count_++;
    Quit(pcb, h);
    break;
  case PROTOCOL_BINARY_CMD_FLUSH:
  case PROTOCOL_BINARY_CMD_FLUSHQ:
    other_count_++;
    Flush(pcb, h);
    break;
  default:
    Unimplemented(h);
    break;
  }
}

void ebbrt::Memcached::Get(NetworkManager::TcpPcb *pcb,
                           protocol_binary_request_header &h, std::unique_ptr<IOBuf> b) {
  // pull key from the request header
  auto p = b->GetDataPointer();
  p.Advance(sizeof(protocol_binary_request_get));
  auto keylenp = reinterpret_cast<char *>(&h.request.keylen);
  //FIXME ntohl:
  auto keylen = (keylenp[1] << 0) | (keylenp[0] << 8); // machine order
  //fixme:
  auto key = std::string(reinterpret_cast<const char *>(p.Data()), keylen);

  // contruct response buffer
  uint16_t status = 0;
  uint8_t extlen = sizeof(uint32_t);
  uint32_t bodylen = extlen;
  auto buf = IOBuf::Create(sizeof(protocol_binary_response_header)+extlen, true);
  auto res = reinterpret_cast<protocol_binary_response_get *>(buf->WritableData());
  res->message.header.response.magic = PROTOCOL_BINARY_RES;
  res->message.header.response.extlen = extlen;

  // check cache for data
  auto query = map_.find(key);
  if (query == map_.end()) {
    // cache miss
    uint16_t statuss = 1;
    auto statusp = reinterpret_cast<char *>(&statuss);
  //FIXME htonl:
    status = (statusp[1] << 0) | (statusp[0] << 8); // network order

    if (h.request.opcode == PROTOCOL_BINARY_CMD_GETQ ||
        h.request.opcode == PROTOCOL_BINARY_CMD_GETKQ)
      return;
    if (h.request.opcode == PROTOCOL_BINARY_CMD_GETK) {
      b->Advance(sizeof(protocol_binary_request_header) + h.request.extlen);
      bodylen += b->Length();
      buf->AppendChain(std::move(b));
    }
  } else {
    // cache hit
    auto kv = query->second->Clone();
    if (h.request.opcode != PROTOCOL_BINARY_CMD_GETK) {
      // Advance or Retreat to cut off key
      res->message.header.response.opcode = PROTOCOL_BINARY_CMD_GET;
      kv->Advance(keylen);
    }else
    {
      res->message.header.response.opcode = PROTOCOL_BINARY_CMD_GETK;
      res->message.header.response.keylen = h.request.keylen;
    }
    bodylen += kv->ComputeChainDataLength();
    buf->AppendChain(std::move(kv));
  }
  res->message.header.response.status = status;
  res->message.header.response.bodylen = htonl(bodylen);
  pcb->Send(std::move(buf));
}

void ebbrt::Memcached::Set(NetworkManager::TcpPcb *pcb,
                           protocol_binary_request_header &h, std::unique_ptr<IOBuf> b) {
  auto p = b->GetDataPointer();
  /// manually correct endianness where necessary
  auto keylenp = reinterpret_cast<char *>(&h.request.keylen);
  ///FIXME:
  auto keylen = (keylenp[1] << 0) | (keylenp[0] << 8);
  auto keyoffset = sizeof(protocol_binary_request_header) + h.request.extlen;
  p.Advance(sizeof(protocol_binary_request_header) + h.request.extlen);
  auto keyptr = reinterpret_cast<const char *>(p.Data());
  p.Advance(keylen);
  auto key = std::string(keyptr, keylen);
  // point buffer to begining of value
  b->Advance(keyoffset);
  map_[key] = std::move(b);
  if (h.request.opcode == PROTOCOL_BINARY_CMD_SET) {
    // construct reply message
    auto buf = IOBuf::Create(sizeof(protocol_binary_response_header), true);
    auto res = reinterpret_cast<protocol_binary_response_header *>(buf->WritableData());
    res->response.magic = PROTOCOL_BINARY_RES;
    res->response.opcode = PROTOCOL_BINARY_CMD_SET;
    pcb->Send(std::move(buf));
  }
}

void ebbrt::Memcached::Quit(NetworkManager::TcpPcb *pcb,
                            protocol_binary_request_header &h) {
  if (h.request.opcode == PROTOCOL_BINARY_CMD_QUIT) {
    // construct reply message
    auto buf = IOBuf::Create(sizeof(protocol_binary_response_header), true);
    auto res = reinterpret_cast<protocol_binary_response_header *>(buf->WritableData());
    res->response.magic = PROTOCOL_BINARY_RES;
    res->response.opcode = PROTOCOL_BINARY_CMD_QUIT;
    pcb->Send(std::move(buf));
#ifdef __EBBRT_ENABLE_TRACE__
        ebbrt::trace::Disable();
        ebbrt::trace::Dump();
#endif
  }
}

void ebbrt::Memcached::Flush(NetworkManager::TcpPcb *pcb,
                            protocol_binary_request_header &h) {
  map_.clear();
  if (h.request.opcode == PROTOCOL_BINARY_CMD_FLUSH) {
    // construct reply message
    auto buf = IOBuf::Create(sizeof(protocol_binary_response_header), true);
    auto res = reinterpret_cast<protocol_binary_response_header *>(buf->WritableData());
    res->response.magic = PROTOCOL_BINARY_RES;
    res->response.opcode = PROTOCOL_BINARY_CMD_FLUSH;
    pcb->Send(std::move(buf));
  }
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
  port_ = port;
  tcp_.Bind(port);
  tcp_.Listen();
  tcp_.Accept([this](NetworkManager::TcpPcb pcb) {
#ifdef __EBBRT_ENABLE_TRACE__
    ebbrt::trace::Enable();
    ebbrt::trace::AddTracepoint(1);
#endif
    auto p = new NetworkManager::TcpPcb(std::move(pcb));
    p->DisableNagle();
    p->Receive([p, this](NetworkManager::TcpPcb &t, std::unique_ptr<IOBuf> b) {

      // check if we've queued a previous partial packet from this netaddr 
      // i.e., "127.0.0.1:8888"
      std::stringstream ss;
      std::string netid;
      ss << t.GetRemoteAddress().addr << ":" << t.GetRemotePort();
      ss >> netid;
      auto it = queued_receives_.find(netid);
      bool in_queue = false;
      std::unique_ptr<IOBuf> *buf;
      if (it != queued_receives_.end()) {
        it->second->Prev()->AppendChain(std::move(b));
        in_queue = true;
        buf = &it->second;
      } else {
        buf = &b;
      }
      if ((*buf)->Length() >= sizeof(protocol_binary_request_header)) {
        auto payload = (*buf)->GetDataPointer();
        auto r = payload.Get<protocol_binary_request_header>();
        auto message_len = sizeof(protocol_binary_request_header)  + htonl(r.request.bodylen);
        auto chain_len = (*buf)->ComputeChainDataLength();
        if (chain_len < message_len && !in_queue) {
          // we need to put the data in the queue
          queued_receives_.emplace(netid, std::move(*buf));
          return;
        } else if (chain_len == message_len) {
          Preexecute(&t, r, std::move((*buf)));
          if (in_queue) {
            queued_receives_.erase(it);
          }
        } else if (chain_len > message_len) {
          kabort("memcached: chain_len[%d] > message_len[%d]\n", chain_len, message_len);
        }
      } else if ((*buf)->Length() == 6) {
        // THIS IS AN UGLY KLUDGE FOR MEMCSLAP SUPPORT
        kprintf("Received Text protocol message of len6 (assuming 'QUIT') \n");
      } else if ((*buf)->Length() >= 1) {
        kbugon(true, "Received Non-Memcached Message, size: %d\n", (*buf)->Length());
      } else {
        delete p;
#ifdef __EBBRT_ENABLE_TRACE__
        ebbrt::trace::AddTracepoint(0);
        ebbrt::trace::Disable();
        ebbrt::trace::Dump();
#endif
      }
    });
    // TCP connection opened
  });
}
