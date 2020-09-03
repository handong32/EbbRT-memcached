#include <cstdlib>
#include <sstream>
#include <ebbrt/SharedIOBufRef.h>
#include <ebbrt/UniqueIOBuf.h>

#include "TcpCommand.h"

ebbrt::TcpCommand::TcpCommand() {}

void ebbrt::TcpCommand::Start(uint16_t port) {
  listening_pcb_.Bind(port, [this](NetworkManager::TcpPcb pcb) {
    // new connection callback
    static std::atomic<size_t> cpu_index{0};
    auto index = 1; //cpu_index.fetch_add(1) % ebbrt::Cpu::Count();
    pcb.BindCpu(index);
    auto connection = new TcpSession(this, std::move(pcb));
    connection->Install();
  });
}

void ebbrt::TcpCommand::TcpSession::Receive(std::unique_ptr<MutIOBuf> b) {
  kassert(b->Length() != 0);

  uint32_t ncores = static_cast<uint32_t>(ebbrt::Cpu::Count());  
  uint32_t mcore = static_cast<uint32_t>(Cpu::GetMine());
  std::string s(reinterpret_cast<const char*>(b->Data()));
  std::string delimiter = ",";
  uint32_t param = 0;
  std::string token1, token2;  
  
  std::size_t pos = s.find(delimiter);
  token1 = s.substr(0, pos);
  token2 = s.substr(pos+1, s.length());
  param = static_cast<uint32_t>(atoi(token2.c_str()));
  ebbrt::kprintf_force("Core: %u TcpCommand::Receive() s=%s token1=%s param=%u\n", mcore, s.c_str(), token1.c_str(), param);
  
  if (token1 == "start") {
    ebbrt::kprintf_force("start()\n");
    for (uint32_t i = 0; i < ncores; i++) {
      network_manager->Config("start_stats", i);
    }    
  } else if (token1 == "stop") {
    ebbrt::kprintf_force("stop()\n");
    for (uint32_t i = 0; i < ncores; i++) {
      network_manager->Config("stop_stats", i);
    }    
  } else if (token1 == "clear") {
    ebbrt::kprintf_force("clear()\n");
    for (uint32_t i = 0; i < ncores; i++) {
      network_manager->Config("clear_stats", i);
    }
  } else if (token1 == "rx_usecs") {
    ebbrt::kprintf_force("itr %u\n", param);
    for (uint32_t i = 0; i < ncores; i++) {
      event_manager->SpawnRemote(
	[token1, param, i] () mutable {
	  network_manager->Config(token1, param);
	}, i);
    }
    
  } else if (token1 == "dvfs") {
    ebbrt::kprintf_force("dvfs %s\n", token2.c_str());
    for (uint32_t i = 0; i < ncores; i++) {
      event_manager->SpawnRemote(
	[param, i] () mutable {
	  // same p state as Linux with performance governor
	  ebbrt::msr::Write(IA32_PERF_CTL, param);    
	}, i);
    }
    
  } else if (token1 == "rapl") {
    ebbrt::kprintf_force("rapl %u\n", param);
    for (uint32_t i = 0; i < 2; i++) {
      event_manager->SpawnRemote(
	[token1, param, i] () mutable {
	  network_manager->Config(token1, param);
	}, i);
    }
    
  } else if (token1 == "rdtsc") {
    uint64_t tstart, tclose;
    std::stringstream ss;
    
    tstart = 0;
    tclose = 0;
    
    for (uint32_t i = 0; i < ncores; i++) {
      if(ixgbe_stats[mcore].rdtsc_start > tstart) {
	tstart = ixgbe_stats[mcore].rdtsc_start;
      }

      if(ixgbe_stats[mcore].rdtsc_end != 0) {
	
	if(tclose == 0) {
	  tclose = ixgbe_stats[mcore].rdtsc_end;
	} else if (ixgbe_stats[mcore].rdtsc_end < tclose) {
	  tclose = ixgbe_stats[mcore].rdtsc_end;
	}
      }
    }
    ss << tstart << ' ' << tclose;
    std::string s = ss.str();
    ebbrt::kprintf_force("rdtsc %s\n", s.c_str());
    auto rbuf = MakeUniqueIOBuf(s.length(), false);
    auto dp = rbuf->GetMutDataPointer();
    std::memcpy(static_cast<void*>(dp.Data()), s.data(), s.length());
    Send(std::move(rbuf));
    
  } else if (token1 == "get") {
    uint8_t* re = (uint8_t*)(ixgbe_logs[param]);
    uint64_t msg_size = ixgbe_stats[param].itr_cnt * sizeof(union IxgbeLogEntry);
    ebbrt::kprintf_force("get msg_size=%lu\n", msg_size);
    while(msg_size > IXGBE_MAX_DATA_PER_TXD) {
      auto buf = std::make_unique<ebbrt::StaticIOBuf>(re, IXGBE_MAX_DATA_PER_TXD);
      Send(std::move(buf));      
      msg_size -= IXGBE_MAX_DATA_PER_TXD;
      re += IXGBE_MAX_DATA_PER_TXD;
    }
    if(msg_size) {
      auto buf = std::make_unique<ebbrt::StaticIOBuf>(re, msg_size);
      Send(std::move(buf));      
    }
    
  } else if (token1 == "test") {
    std::string tmp = "test test test";
    auto rbuf = MakeUniqueIOBuf(tmp.length(), false);
    auto dp = rbuf->GetMutDataPointer();
    std::memcpy(static_cast<void*>(dp.Data()), tmp.data(), tmp.length());
    Send(std::move(rbuf));
    
  } else if (token1 == "print") {
    ebbrt::kprintf_force("core=%u itr_cnt=%d itr_cnt2=%d\n", param, ixgbe_stats[param].itr_cnt, ixgbe_stats[param].itr_cnt2);
    for (uint32_t i = 0; i < 10; i++) {
      ebbrt::kprintf_force("%d %d %d %d %d %llu %llu %llu %llu %llu %llu %llu %llu %llu\n",
			   i,
			   ixgbe_logs[param][i].Fields.rx_desc, ixgbe_logs[param][i].Fields.rx_bytes,
			   ixgbe_logs[param][i].Fields.tx_desc, ixgbe_logs[param][i].Fields.tx_bytes,
			   ixgbe_logs[param][i].Fields.ninstructions,
			   ixgbe_logs[param][i].Fields.ncycles,
			   ixgbe_logs[param][i].Fields.nref_cycles,
			   ixgbe_logs[param][i].Fields.nllc_miss,
			   ixgbe_logs[param][i].Fields.c3,
			   ixgbe_logs[param][i].Fields.c6,
			   ixgbe_logs[param][i].Fields.c7,
			   ixgbe_logs[param][i].Fields.joules,		   
			   ixgbe_logs[param][i].Fields.tsc);
    }
    ebbrt::kprintf_force("*******\n");
    for (uint32_t i = (ixgbe_stats[param].itr_cnt-10); i < ixgbe_stats[param].itr_cnt; i++) {
      ebbrt::kprintf_force("%d %d %d %d %d %llu %llu %llu %llu %llu %llu %llu %llu %llu\n",
			   i,
			   ixgbe_logs[param][i].Fields.rx_desc, ixgbe_logs[param][i].Fields.rx_bytes,
			   ixgbe_logs[param][i].Fields.tx_desc, ixgbe_logs[param][i].Fields.tx_bytes,
			   ixgbe_logs[param][i].Fields.ninstructions,
			   ixgbe_logs[param][i].Fields.ncycles,
			   ixgbe_logs[param][i].Fields.nref_cycles,
			   ixgbe_logs[param][i].Fields.nllc_miss,
			   ixgbe_logs[param][i].Fields.c3,
			   ixgbe_logs[param][i].Fields.c6,
			   ixgbe_logs[param][i].Fields.c7,
			   ixgbe_logs[param][i].Fields.joules,		   
			   ixgbe_logs[param][i].Fields.tsc);
    }
  } else {
    ebbrt::kprintf_force("Unknown command %s\n", token1.c_str());
  }
  

  // reply buffer pointer  
  /*std::string tmp = "test test test";
  auto rbuf = MakeUniqueIOBuf(tmp.length(), false);
  auto dp = rbuf->GetMutDataPointer();
  std::memcpy(static_cast<void*>(dp.Data()), tmp.data(), tmp.length());  

  Send(std::move(rbuf));*/
  
  /*uint8_t* re = (uint8_t*)(ixgbe_logs[13]);
  uint64_t msg_size = 1000000 * sizeof(union IxgbeLogEntry);
  uint64_t sum = 0;
  for(uint64_t i = 0; i < IXGBE_TSO_LIMIT; i++) {
    sum += re[i];
  }
  ebbrt::kprintf_force("SendLog() sum=%lu\n", sum);
  ebbrt::kprintf_force("SendLog() msg_size=%llu\n", msg_size);
  while(msg_size > IXGBE_MAX_DATA_PER_TXD) {
    auto buf = std::make_unique<ebbrt::StaticIOBuf>(re, IXGBE_MAX_DATA_PER_TXD);
    Send(std::move(buf));      
    msg_size -= IXGBE_MAX_DATA_PER_TXD;
    re += IXGBE_MAX_DATA_PER_TXD;
  }
  if(msg_size) {
    auto buf = std::make_unique<ebbrt::StaticIOBuf>(re, msg_size);
    Send(std::move(buf));      
    }*/
}


