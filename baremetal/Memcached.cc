//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
#include <cstdlib>
#include <ebbrt/Debug.h>
#include <ebbrt/Messenger.h>
#include "Memcached.h"

ebbrt::Memcached::Memcached() {} 

const char* ebbrt::Memcached::com2str(uint8_t cmd) {
  static const char* const text[] = {
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

void ebbrt::Memcached::Preexecute(NetworkManager::TcpPcb* pcb,
                                  protocol_binary_request_header& h,
                                  Buffer b) {
  switch (h.request.opcode) {
  case PROTOCOL_BINARY_CMD_SET:
    SetCmd(pcb, h, std::move(b));
    break;
  case PROTOCOL_BINARY_CMD_GETK:
  case PROTOCOL_BINARY_CMD_GET:
    GetCmd(pcb, h, std::move(b));
    break;
  default:
    Unimplemented(pcb, h);
    break;
  }
}

void ebbrt::Memcached::GetCmd(NetworkManager::TcpPcb* pcb,
                              protocol_binary_request_header& h, Buffer b) {
  auto p = b.GetDataPointer();
  // grab information from the request header
  auto keylenp = reinterpret_cast<char*>(&h.request.keylen);
  auto keylen = (keylenp[1] << 0) | (keylenp[0] << 8); // machine order
  p.Advance(sizeof(protocol_binary_request_get));
  //auto keyptr = static_cast<char*>(p.Addr());
  auto key = std::string(static_cast<char*>(p.Addr()), keylen);
  
  // DEBUG: print out the requestd key
  kprintf("GET %s\n", key.c_str());
  /////////

  // TODO: handle key not found
  auto val = map_[key];

  // contruct response buffer
  uint8_t extlen = sizeof(uint32_t);
  uint32_t bodylen = val->size() + extlen;  // key + value + extra
  auto bodylenp = reinterpret_cast<char*>(&bodylen);
  uint32_t bodylen_no = (bodylenp[3] << 0) | (bodylenp[2] << 8) |
                        (bodylenp[1] << 16) |
                        (bodylenp[0] << 24);  // network order

  auto buf = Buffer(sizeof(protocol_binary_response_header) + extlen, true);
  kprintf("buff size: %d\n", buf.size());
  ;
  kprintf("protocol size: %d\n", sizeof(protocol_binary_response_get));
  auto res = static_cast<protocol_binary_response_get*>(buf.data());
  res->message.header.response.magic = PROTOCOL_BINARY_RES;
  res->message.header.response.opcode = PROTOCOL_BINARY_CMD_GETK;
  res->message.header.response.keylen = h.request.keylen;
  res->message.header.response.extlen = extlen;  // fix
  res->message.header.response.bodylen = bodylen_no;
  // append value,key to response buffer
  buf.append(val);
  pcb->Send(std::make_shared<const Buffer>(std::move(buf)));
}

void ebbrt::Memcached::SetCmd(NetworkManager::TcpPcb* pcb,
                              protocol_binary_request_header& h, Buffer b) {
  auto p = b.GetDataPointer();
  /// manually correct endianness where necessary
  auto keylenp = reinterpret_cast<char*>(&h.request.keylen);
  auto keylen = (keylenp[1] << 0) | (keylenp[0] << 8);
  auto keyoffset =
      sizeof(protocol_binary_request_header) + h.request.extlen;
  p.Advance(sizeof(protocol_binary_request_header) + h.request.extlen);
  auto keyptr = static_cast<char*>(p.Addr());
  p.Advance(keylen);
  auto key = std::string(keyptr, keylen);

  /////////////// DEBUG -- DELETE THIS EVENTUALLY
  //auto bodylenp = reinterpret_cast<char*>(&h.request.bodylen);
  //auto valptr = static_cast<char*>(p.Addr());
  //auto vallen = bodylen - keylen - h.request.extlen;
  //auto bodylen = (bodylenp[3] << 0) | (bodylenp[2] << 8) | (bodylenp[1] << 16) |
    //             (bodylenp[0] << 24);
  //std::vector<unsigned char> val(vallen);
  //std::copy(valptr, valptr + vallen, val.begin());
  //kprintf("SET %s\n", key.c_str());
  //for (auto c : val)
  //  kprintf("%c", c);
  //kprintf("\n");
  ///////////////

  // point buffer to value data
  b += keyoffset;
  // data into array
  map_[key] = std::make_shared<Buffer>(std::move(b));

  // construct a SET reply message
  auto buf = Buffer(sizeof(protocol_binary_response_header));
  auto res = static_cast<protocol_binary_response_header*>(buf.data());
  for (uint8_t i = 0; i < sizeof(protocol_binary_response_header); i++)
    res->bytes[i] = 0;
  res->response.magic = PROTOCOL_BINARY_RES;
  res->response.opcode = PROTOCOL_BINARY_CMD_SET;
  pcb->Send(std::make_shared<const Buffer>(std::move(buf)));
}

void ebbrt::Memcached::Unimplemented(NetworkManager::TcpPcb* pcb,
                                     protocol_binary_request_header& h) {
  const char* cmd = com2str(h.request.opcode);
  kprintf("%s CMD IS NOT SUPPORTED\n", cmd);
}

void ebbrt::Memcached::StartListening(uint16_t port) {
  ebbrt::kprintf("Memcache ebb starts listening\n");
  port_ = port;
  tcp_.Bind(port);
  tcp_.Listen();
  tcp_.Accept([this](NetworkManager::TcpPcb pcb) {
    auto p = new NetworkManager::TcpPcb(std::move(pcb));
    p->Receive([p, this](NetworkManager::TcpPcb& t, Buffer b) {
      kbugon(b.length() > 1, "handle multiple length buffer\n");
      if (b.size() >= sizeof(protocol_binary_request_header)) {
        auto payload = b.GetDataPointer();
        auto &r = payload.Get<protocol_binary_request_header>();
        const char* cmd = com2str(r.request.opcode);
        if (cmd != NULL) {
          Preexecute(&t, r, std::move(b));
        } else {
          kprintf("Received Unknown Command #%d\n", r.request.opcode);
        }
      } else if (b.size() >= 1) {
        kprintf("Received Non-Memcached Message\n");
      } else {
        kprintf("TCP Connection closed\n");
        std::free(p);
      }
    });
    kprintf("TCP connection opened\n");
  });
}
