//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
#include <cstdlib>
#include <sstream>
#include <ebbrt/SharedIOBufRef.h>
#include <ebbrt/UniqueIOBuf.h>

#include "Memcached.h"

ebbrt::Memcached::Memcached() {}

ebbrt::Memcached::GetResponse::GetResponse() {}

ebbrt::Memcached::GetResponse::GetResponse(std::unique_ptr<IOBuf> b) {
  auto bptr = b.get();
  auto remainder = b->Next();
  request_ =
      IOBuf::Create<MutSharedIOBufRef>(SharedIOBufRef::CloneView, std::move(b));
  while (remainder != bptr) {
    auto next = remainder->Next();
    auto ref = IOBuf::Create<IOBufRef>(IOBufRef::CloneView, *remainder);
    request_->PrependChain(std::move(ref));
    remainder = next;
  }
  binary_response_ = nullptr;
  binary_ = true;
}

std::unique_ptr<ebbrt::IOBuf> ebbrt::Memcached::GetResponse::Ascii() {
  kassert(request_ != nullptr);
  return nullptr;
}

std::unique_ptr<ebbrt::IOBuf> ebbrt::Memcached::GetResponse::Binary() {
  kassert(request_ != nullptr);
  if (binary_response_ == nullptr) {
    //  We optimise  binary response for the GETK which requires extras, key and
    // value. Format of stored request is <extra,key,value>
    kassert(binary_ == true); // We do not yet support mixed ASCII and BINARY
    auto rptr = request_.get();
    auto remainder = request_->Next();
    binary_response_ =
        IOBuf::Create<MutSharedIOBufRef>(SharedIOBufRef::CloneView, *request_);
    while (remainder != rptr) {
      auto next = remainder->Next();
      auto ref = IOBuf::Create<IOBufRef>(IOBufRef::CloneView, *remainder);
      binary_response_->PrependChain(std::move(ref));
      remainder = next;
    }
    binary_response_->AdvanceChain(sizeof(protocol_binary_request_header));
    // auto md = binary_response_->GetMutDataPointer();
    // TODO: clear flag bits
    // md.Get<uint32_t>() = 0;
    binary_response_->AdvanceChain(sizeof(uint32_t));
  }
  auto brptr = binary_response_.get();
  auto remainder = binary_response_->Next();
  auto ret = IOBuf::Create<MutSharedIOBufRef>(SharedIOBufRef::CloneView,
                                              *binary_response_);
  while (remainder != brptr) {
    auto next = remainder->Next();
    auto ref = IOBuf::Create<IOBufRef>(IOBufRef::CloneView, *remainder);
    ret->PrependChain(std::move(ref));
    remainder = next;
  }
  return std::move(ret);
}

ebbrt::Memcached::GetResponse *ebbrt::Memcached::Get(std::unique_ptr<IOBuf> b,
                                                     std::string key) {
  auto p = table_.find(key);
  if (!p) {
    // cache miss
    return nullptr;
  } else {
    // cache hit
    return &(p->value);
  }
}

void ebbrt::Memcached::Set(std::unique_ptr<IOBuf> b, std::string key) {
  std::lock_guard<ebbrt::SpinLock> guard(table_lock_);
  table_.insert(*new TableEntry(key, std::move(b)));
  return;
}

void ebbrt::Memcached::Quit() {
  return;
}

void ebbrt::Memcached::Flush() {
  // TODO
  kprintf("warning: rcu flush is unimplemented");
  return;
}

static const char *const ascii_reply[] = { "VALUE ", "STORED\r\n",
                                           "NOT_STORED\r\n", "EXISTS\r\n",
                                           "NOT_FOUND\r\n", "DELETED", "TOUCHED"
                                                                       "ERROR",
                                           "END", "CLIENT_ERROR",
                                           "SERVER_ERROR" };

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

void ebbrt::Memcached::Start(uint16_t port) {
  listening_pcb_.Bind(port, [this](NetworkManager::TcpPcb pcb) {
    // new connection callback
    static std::atomic<size_t> cpu_index{0};
    auto index = cpu_index.fetch_add(1) % ebbrt::Cpu::Count();
    pcb.BindCpu(index);
    auto connection = new TcpSession(this, std::move(pcb));
    connection->Install();
  });
}

void ebbrt::Memcached::TcpSession::Receive(std::unique_ptr<MutIOBuf> b) {

  kassert(b->Length() != 0);
  // restore any queued buffers
  if (buf_) {
    buf_->PrependChain(std::move(b));
  } else {
    buf_ = std::move(b);
  }

  // reply buffer pointer
  std::unique_ptr<MutIOBuf> rbuf(nullptr);

  // process buffer chain
  while (buf_) {
    // inspeact buffer head
    //auto bp = buf_->Data();
    auto dp = buf_->GetDataPointer();
    auto chain_len = buf_->ComputeChainDataLength();

    // set protocol {binary, ascii} specifics
    unsigned int head_len, body_len, message_len;
    auto magic = dp.GetNoAdvance(1);

    if (*magic == PROTOCOL_BINARY_REQ) {
      head_len = sizeof(protocol_binary_request_header);
      // Do we have enough data for a header?
      if (chain_len < head_len) {
        break; // preservinf partial packer in buf_
      }
      auto h = dp.Get<protocol_binary_request_header>();
      body_len = htonl(h.request.bodylen);
      message_len = head_len + body_len;
    } else {
      kprintf("Unknown msg header. buf length:%u \n",(unsigned int)buf_->Length());
      return;
    }

    // start processing requests
    std::unique_ptr<MutIOBuf> msg;

    if (likely(chain_len == message_len)) {
      // We have a full message
      msg = std::move(buf_);
    } else if (chain_len > message_len) {
      // Handle the case when we've received multiple message in our buffer
      // chain
      //
      // After this loop msg should hold exactly one message and everything
      // else will be in buf_
      bool first = true;
      msg = std::move(buf_);
      for (auto &buf : *msg) {
        // for each buffer
        auto buf_len = buf.Length();
        if (buf_len == message_len) {
          // If the first buffer contains the full message
          // Move the remainder of chain into buf_, while our message remains
          // in msg_
          buf_ = std::unique_ptr<MutIOBuf>(
              static_cast<MutIOBuf *>(msg->UnlinkEnd(*buf.Next()).release()));
          break;
        } else if (buf_len > message_len) {
          // Here we need to split the buffer
          std::unique_ptr<MutIOBuf> end;
          if (first) {
            end = std::move(msg);
          } else {
            auto tmp_end =
                static_cast<MutIOBuf *>(msg->UnlinkEnd(buf).release());
            end = std::unique_ptr<MutIOBuf>(tmp_end);
          }
          auto remainder = end->Pop();
          // make a reference counted IOBuf to the end
          auto rc_end = IOBuf::Create<MutSharedIOBufRef>(
              SharedIOBufRef::CloneView, std::move(end));
          // create a copy (increments ref count)
          buf_ = IOBuf::Create<MutSharedIOBufRef>(SharedIOBufRef::CloneView,
                                                  *rc_end);
          // trim and append to msg
          rc_end->TrimEnd(buf_len - message_len);
          if (first) {
            msg = std::move(rc_end);
          } else {
            msg->PrependChain(std::move(rc_end));
          }

          // advance to start of next message
          buf_->Advance(message_len);
          if (remainder)
            buf_->PrependChain(std::move(remainder));
          break;
        }
        message_len -= buf_len;
        first = false;
      } // end for(buf:msg)
    } else {
      // since message_len > chain_len we simply wait for more data
      break;
    }

    // msg now holds exactly one message
    std::unique_ptr<IOBuf> replybuf(nullptr);
    auto reply = MakeUniqueIOBuf(sizeof(protocol_binary_response_header), true);
    // fixme: pass mutdatapointer instead of pointer
    auto rehead =
        reinterpret_cast<protocol_binary_response_header *>(reply->MutData());
    replybuf = mcd_->ProcessBinary(std::move(msg), rehead);
    // We send the response if response.magic is set,
    if (rehead->response.magic == PROTOCOL_BINARY_RES) {
      if (replybuf) {
        reply->PrependChain(std::move(replybuf));
      }
      // queue data to send
      if (rbuf == nullptr) {
        rbuf = std::move(reply);
      } else {
        rbuf->PrependChain(std::move(reply));
      }


      while (rbuf->ComputeChainDataLength() > 1460) {
        size_t remaining_length = 1460;
        for (auto &buf : *rbuf) {
          kassert(buf.Length() <= 1460);
          if (buf.Length() > remaining_length) {
            auto remainder = std::unique_ptr<MutIOBuf>(
                static_cast<MutIOBuf *>(rbuf->UnlinkEnd(buf).release()));
            Send(std::move(rbuf));
            rbuf = std::move(remainder);
            break;
          } else {
            remaining_length -= buf.Length();
          }
        }

      }
    }
  } // end while(buf_)

  if (rbuf != nullptr) {
    kassert(rbuf->ComputeChainDataLength() <= 1460);
    Send(std::move(rbuf));
  }

  return;
}

std::unique_ptr<ebbrt::IOBuf>
ebbrt::Memcached::ProcessBinary(std::unique_ptr<IOBuf> buf,
                                protocol_binary_response_header *rhead) {
  ebbrt::Memcached::GetResponse *res; // response buffer
  std::unique_ptr<IOBuf> kv(nullptr); // key-value IObuf
  std::string key;
  uint32_t bodylen = 0;
  uint16_t status = 0;

  auto bdata = buf->GetDataPointer();
  // pull data from incoming header
  auto h = bdata.Get<protocol_binary_request_header>();
  int32_t keylen = ntohl(h.request.keylen << 16);
  // pull key into string
  bdata.Advance(h.request.extlen);
  auto keyptr = bdata.Get(keylen);

  if (keylen > 0) {
    key = std::string(reinterpret_cast<const char *>(keyptr), keylen);
  }

  // set response header defaults
  // we use magic as a signal to send or remaining quiet
  rhead->response.magic = 0;
  rhead->response.opcode = h.request.opcode;
  switch (h.request.opcode) {
  case PROTOCOL_BINARY_CMD_SET:
    rhead->response.magic = PROTOCOL_BINARY_RES;
  // no break
  case PROTOCOL_BINARY_CMD_SETQ:
    Set(std::move((buf)), key);
    return nullptr;
  case PROTOCOL_BINARY_CMD_GET:
  case PROTOCOL_BINARY_CMD_GETQ:
  case PROTOCOL_BINARY_CMD_GETK:
  case PROTOCOL_BINARY_CMD_GETKQ:
    rhead->response.magic = PROTOCOL_BINARY_RES;
    // binary() returns <ext, key, value>
    rhead->response.extlen = sizeof(uint32_t);
    res = Get(std::move((buf)), key);
    if (res) {
      // Hit
      // GetResponse::Binary() returns IOBuf containing <ext, key, value>
      kv = res->Binary();
      bodylen += kv->ComputeChainDataLength();
    } else {
      // Miss
      if (h.request.opcode == PROTOCOL_BINARY_CMD_GETQ ||
          h.request.opcode == PROTOCOL_BINARY_CMD_GETKQ) {
        // If GETQ/GETKQ we send no response
        rhead->response.magic = 0;
        return nullptr;
      } else if (h.request.opcode == PROTOCOL_BINARY_CMD_GETK) {
        // inthe case of GETK miss, we need to reply with key
        // auto keybuf = MakeUniqueIOBuf(keylen, true);
        // auto keybufptr = keybuf->GetMutDataPointer();
        // std::memcpy(keybufptr.Data(), key.c_str(), keylen);
        // bodylen += keylen;
        // kv = std::move(keybuf);
      }
      // return miss response status
      keylen = 0;
      rhead->response.extlen = 0;
      status = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    }
    break;
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
