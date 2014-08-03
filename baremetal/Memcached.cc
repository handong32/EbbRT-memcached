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


ebbrt::Memcached::Memcached(){}

ebbrt::Memcached::GetResponse::GetResponse(){}

ebbrt::Memcached::GetResponse::GetResponse(std::unique_ptr<IOBuf> b){
  request_ = std::move(b);
  ascii_response_ = nullptr;
  binary_response_ = nullptr;
}

std::unique_ptr<ebbrt::IOBuf> ebbrt::Memcached::GetResponse::Ascii() {
  kassert(request_ != nullptr);
  return nullptr;
}

std::unique_ptr<ebbrt::IOBuf> ebbrt::Memcached::GetResponse::Binary() {
  kassert(request_ != nullptr);
  if(binary_response_ == nullptr){
    // optimise  binary response for the GETK which requires extras, key and
    // value. Format of stored request is  <extra,key,value>

    // FIXME: assumes the request was binary 
    binary_response_ = request_->Clone();
    binary_response_->Advance(sizeof(protocol_binary_request_header)+4);
    // clear extras byte (is this nessessary?)
    auto extrap = reinterpret_cast<uint32_t *>(binary_response_->WritableData());
    *extrap = 0; 
  }
  return binary_response_->Clone();
}

ebbrt::Memcached::GetResponse*
ebbrt::Memcached::Get(std::unique_ptr<IOBuf> b, std::string key) {
  auto query = map_.find(key);
  if (query == map_.end()) {
    // cache miss
    return nullptr;
  } else {
    // cache hit 
    return &query->second;
  }
}

void ebbrt::Memcached::Set(std::unique_ptr<IOBuf> b, std::string key){
  map_[key] = GetResponse(std::move(b));
  return;
}

void ebbrt::Memcached::Quit() {
  // TODO 
  return;
}

void ebbrt::Memcached::Flush() {
  map_.clear();
  return;
}

static const char *const ascii_reply[] = {
"VALUE ",
"STORED\r\n", 
"NOT_STORED\r\n",
"EXISTS\r\n",
"NOT_FOUND\r\n",
"DELETED",
"TOUCHED"
"ERROR", 
"END",
"CLIENT_ERROR",
"SERVER_ERROR"
}


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

void ebbrt::Memcached::Nop(protocol_binary_request_header &h) {
  const char *cmd = com2str(h.request.opcode);
  kprintf("%s CMD IS NOP\n", cmd);
}

void ebbrt::Memcached::Unimplemented(protocol_binary_request_header &h) {
  const char *cmd = com2str(h.request.opcode);
  kbugon(true, "%s CMD IS UNSUPPORTED. ABORTING...\n", cmd);
}

void ebbrt::Memcached::StartListening(uint16_t port) {
  //listening_port_ = port;
  tcp_.Bind(port);
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
  // restore any queue message fragments
  if (queued_bufs_ != nullptr) {
    queued_bufs_->Prev()->AppendChain(std::move(b));
    buf = &queued_bufs_;
    in_queue = true;
  } else {
    buf = &b;
  }
  auto bp = ((*buf))->Data();
  std::unique_ptr<IOBuf> rbuf(nullptr);
  // check for binary protocol
  if (bp[0] == PROTOCOL_BINARY_REQ){ 
    auto chain_len = (*buf)->ComputeChainDataLength();
    auto bdata = (*buf)->GetDataPointer();
    auto h = bdata.Get<protocol_binary_request_header>();
    // confirm we have the full message
    auto message_len =
      sizeof(protocol_binary_request_header) + htonl(h.request.bodylen);
    if (chain_len < message_len) {
      if (!in_queue)
        queued_bufs_ = std::move((*buf));
      return;
    } else {
      kassert(chain_len == message_len);
      if (in_queue) { queued_bufs_.reset(nullptr); }
      // process the TCP request
      auto reply = IOBuf::Create(sizeof(protocol_binary_response_header), true);
      auto rehead = reinterpret_cast<protocol_binary_response_header*>(
          reply->WritableData());
      rbuf = mcd_->ProcessBinary(std::move((*buf)), rehead);
      if (rehead->response.magic == PROTOCOL_BINARY_RES) {
        if (rbuf) {
          reply->AppendChain(std::move(rbuf));
        }
        t.Send(std::move(reply));
      }
      return;
    }
  } else {
    // check for ascii protcol
    auto bufstr = (*buf)->ToString();
    // extract ascii command line
    auto head = bufstr.substr(0, bufstr.find("\r\n"));
    kbugon(head.empty());
    // we have received a full request
    rbuf = mcd_->ProcessAscii(std::move((*buf)), head);
    if (rbuf) {
      kprintf("sending ascii reply\n");
      // t.Send(std::move(rbuf));
    }
    return;
  }
}

std::unique_ptr<ebbrt::IOBuf>
ebbrt::Memcached::ProcessAscii(std::unique_ptr<IOBuf> buf, std::string head){

  // commands: set, add, replace, prepend, cas, get, gets, delete, incr, decr,
  // touch
  auto cmd_end = head.find(' ', 0);
  auto cmd = head.substr(0, cmd_end);

  if (cmd == "set") {
    auto key_end = head.find(' ', cmd_end + 1);
    auto flag_end = head.find(' ', key_end + 1);
    auto exp_end = head.find(' ', flag_end + 1);
    auto size_end = head.find(' ', exp_end + 1);
    //// todo: optional noreply
    auto key = head.substr(cmd_end + 1, key_end - cmd_end - 1);
    // auto flag = head.substr(key_end + 1, flag_end - key_end - 1);
    // auto exp = head.substr(flag_end + 1, exp_end - flag_end - 1);
    auto size = head.substr(exp_end + 1, size_end - exp_end - 1);
    auto key_len = key.length();
    auto message_len = key_len + atoi(size.c_str()) + 4; // 4 for deliminators

    //// confirm we have the full message
    //if (chain_len < message_len) {
    //  if (!in_queue)
    //    queued_bufs_ = std::move((*buf));
    //  return;
    //} else if (chain_len > message_len) {
    //  kabort("memcached ascii: chain_len[%d] > message_len[%d]\n", chain_len,
    //         message_len);
    //}
    // looks like we have a good set
    mcd_->Set(std::move((*buf)), key);
    kprintf("set: %s\n", head.c_str());
  } else if (cmd == "get")
    kprintf("get: %s\n", head.c_str());
  else if (cmd == "gets")
    kprintf("gets: %s\n", head.c_str());
  else if (cmd == "quit")
    kprintf("quit:%s\n", head.c_str());
  else
    kprintf("cmd:%s unimplemented\n", cmd.c_str());
  // end ascii processing
  return nullptr;
}

std::unique_ptr<ebbrt::IOBuf>
ebbrt::Memcached::ProcessBinary(std::unique_ptr<IOBuf> buf, protocol_binary_response_header* rhead) {

    ebbrt::Memcached::GetResponse* res;
    std::unique_ptr<IOBuf> kv(nullptr);
    std::string key;
    auto bdata = buf->GetDataPointer();
    auto h = bdata.Get<protocol_binary_request_header>();
    uint32_t keylen = ntohl(h.request.keylen << 16);
    auto keyoffset = sizeof(protocol_binary_request_header) + h.request.extlen;
    // incase of a getk miss, we make a copy of the key to return
    auto keybuf = IOBuf::Create(keylen, true);
    auto keyptr = reinterpret_cast<char *>(keybuf->WritableData());

    rhead->response.magic = 0;
    rhead->response.opcode = h.request.opcode;
    uint32_t bodylen = 0;
    uint16_t status = 0;

    switch (h.request.opcode) {
      case PROTOCOL_BINARY_CMD_SET:
        rhead->response.magic = PROTOCOL_BINARY_RES;
        // no break
      case PROTOCOL_BINARY_CMD_SETQ:
        buf->Advance(keyoffset); 
        key = std::string(reinterpret_cast<const char *>(buf->Data()), keylen);
        buf->Retreat(keyoffset); 
        Set(std::move((buf)), key);
        return nullptr;
      case PROTOCOL_BINARY_CMD_GET:
        rhead->response.magic = PROTOCOL_BINARY_RES;
        // no break
      case PROTOCOL_BINARY_CMD_GETQ:
        buf->Advance(keyoffset); 
        key = std::string(reinterpret_cast<const char *>(buf->Data()), keylen);
        buf->Retreat(keyoffset); 
        res = Get(std::move((buf)), key);
        rhead->response.extlen = sizeof(uint32_t);
        bodylen = sizeof(uint32_t);
        if (res) {
          kv = res->Binary();
          // remove the key from the response buffer
          kv->Advance(keylen); 
          auto writer = reinterpret_cast<int*>(kv->WritableData());
          *writer = 0;
          bodylen += kv->ComputeChainDataLength();
        } else {
          status = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
          if(h.request.opcode == PROTOCOL_BINARY_CMD_GETQ)
            return nullptr;
        }
        break;
      case PROTOCOL_BINARY_CMD_GETK:
        rhead->response.magic = PROTOCOL_BINARY_RES;
        // incase of miss, create copy of key
        buf->Advance(keyoffset);
        key = std::string(reinterpret_cast<const char *>(buf->Data()), keylen);
        std::memcpy(keyptr, reinterpret_cast<const char *>((buf)->Data()),
                    keylen);
        buf->Retreat(keyoffset);
        rhead->response.extlen = sizeof(uint32_t);
        res = Get(std::move((buf)), key);
        if (res) {
          kv = res->Binary();
          // binary() returns <ext, key, value>
          bodylen += kv->ComputeChainDataLength();
        } else {
          // return miss response + key
          status = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
          bodylen += keybuf->Length();
          kv = std::move(keybuf);
        }
        break;
     // case PROTOCOL_BINARY_CMD_GETKQ:
     //   Get(std::move((buf)), keylen);
     //   break;
      case PROTOCOL_BINARY_CMD_NOOP:
      case PROTOCOL_BINARY_CMD_QUIT:
        Quit();
        break;
      case PROTOCOL_BINARY_CMD_QUITQ:
        Quit();
        return nullptr;
      case PROTOCOL_BINARY_CMD_FLUSH:
        Flush();
        break;
      case PROTOCOL_BINARY_CMD_FLUSHQ:
        Flush();
        return nullptr;
      default:
        Unimplemented(h);
        return nullptr;
    }

    rhead->response.keylen = (htonl(keylen) >> 16);
    rhead->response.status = (htonl(status) >> 16);
    rhead->response.bodylen = htonl(bodylen);
    return kv;
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
