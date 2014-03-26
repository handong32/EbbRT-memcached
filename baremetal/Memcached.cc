//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
#include <cstdlib>
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
  auto keylen = (keylenp[1] << 0) | (keylenp[0] << 8); // machine order
  auto key = std::string(reinterpret_cast<const char *>(p.Data()), keylen);

  // contruct response buffer
  uint16_t status = 0;
  uint8_t extlen = sizeof(uint32_t);
  uint32_t bodylen = extlen;
  auto buf = IOBuf::Create(sizeof(protocol_binary_response_header)+extlen, true);
  auto res = reinterpret_cast<protocol_binary_response_get *>(buf->WritableData());
  res->message.header.response.magic = PROTOCOL_BINARY_RES;
  res->message.header.response.opcode = PROTOCOL_BINARY_CMD_GETK;
  res->message.header.response.extlen = extlen;

  // check cache for data
  auto query = map_.find(key);
  if (query == map_.end()) {
    // cache miss
    uint16_t statuss = 1;
    auto statusp = reinterpret_cast<char *>(&statuss);
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
      kv->Advance(sizeof(key));
    }else
    {
      res->message.header.response.keylen = h.request.keylen;
    }
    bodylen += kv->Length();
    buf->AppendChain(std::move(kv));
  }

  // network order
  auto bodylenp = reinterpret_cast<char *>(&bodylen);
  uint32_t bodylen_no = (bodylenp[3] << 0) | (bodylenp[2] << 8) |
                        (bodylenp[1] << 16) | (bodylenp[0] << 24);
  res->message.header.response.status = status;
  res->message.header.response.bodylen = bodylen_no;
  pcb->Send(std::move(buf));
}

void ebbrt::Memcached::Set(NetworkManager::TcpPcb *pcb,
                           protocol_binary_request_header &h, std::unique_ptr<IOBuf> b) {
  auto p = b->GetDataPointer();
  /// manually correct endianness where necessary
  auto keylenp = reinterpret_cast<char *>(&h.request.keylen);
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

void ebbrt::Memcached::Nop(protocol_binary_request_header &h) {
  const char *cmd = com2str(h.request.opcode);
  kprintf("%s CMD IS NOP\n", cmd);
}

void ebbrt::Memcached::Unimplemented(protocol_binary_request_header &h) {
  const char *cmd = com2str(h.request.opcode);
  kbugon(true, "%s CMD IS UNSUPPORTED. ABORTING...\n", cmd);
}

void ebbrt::Memcached::StartListening(uint16_t port) {
  ebbrt::kprintf("Memcache ebb starts listening\n");
  port_ = port;
  tcp_.Bind(port);
  tcp_.Listen();
  tcp_.Accept([this](NetworkManager::TcpPcb pcb) {
    auto p = new NetworkManager::TcpPcb(std::move(pcb));
    p->Receive([p, this](NetworkManager::TcpPcb &t, std::unique_ptr<IOBuf> b) {
      kbugon(b->IsChained(), "cannot handle multiple length buffer\n");
      if (b->Length() >= sizeof(protocol_binary_request_header)) {
        auto payload = b->GetDataPointer();
        auto r = payload.Get<protocol_binary_request_header>();
        Preexecute(&t, r, std::move(b));
      } else if (b->Length() == 6) {
        // THIS IS AN UGLY KLUDGE FOR MEMCSLAP SUPPORT
        kprintf("Received Text protocol message of len6 (assuming 'QUIT') \n");
      } else if (b->Length() >= 1) {
        kbugon(true, "Received Non-Memcached Message, size: %d\n", b->Length());
      } else {
        kprintf("TCP Connection closed\n");
        delete p;
      }
    });
    // TCP connection opened
  });
}
