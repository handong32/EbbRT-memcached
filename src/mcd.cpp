#include <ebbrt/Debug.h>
#include <ebbrt/EbbAllocator.h>
#include <ebbrt/native/Net.h>
#include <ebbrt/native/Msr.h>
#include <ebbrt/native/EventManager.h>
#include <ebbrt/native/Cpu.h>

#include "Memcached.h"
#include "TcpCommand.h"
//#include "TcpSender.h"

#define MCDPORT 11211

//ebbrt::NetworkManager::TcpPcb tpcb;
//std::unique_ptr<TcpSender> handler;

void AppMain()
{
  uint32_t ncores = static_cast<uint32_t>(ebbrt::Cpu::Count());  
  for (uint32_t i = 0; i < ncores; i++) {
    ebbrt::Promise<void> p;
    auto f = p.GetFuture();
    ebbrt::event_manager->SpawnRemote(
      [ncores, i, &p] () mutable {
	// disables turbo boost, thermal control circuit
	ebbrt::msr::Write(IA32_MISC_ENABLE, 0x4000850081);
	// same p state as Linux with performance governor
	ebbrt::msr::Write(IA32_PERF_CTL, 0x1D00);

	uint64_t ii, jj, sum=0, sum2=0;
	for(ii=0;ii<ncores;ii++) {	  
	  for(jj=0;jj<IXGBE_LOG_SIZE;jj++) {
	    sum += ixgbe_logs[ii][jj].Fields.tsc;
	  }
	  
	  uint8_t* ptr = bsendbufs[ii]->MutData();
	  for(jj=0;jj<IXGBE_MAX_DATA_PER_TXD;jj++) {
	    sum2 += ptr[ii];
	  }
	}

	ebbrt::kprintf_force("Cpu=%u Sum=%llu Sum2=%llu\n", i, sum, sum2);
	p.SetValue();
      }, i);
    f.Block();
  }
  
  auto id = ebbrt::ebb_allocator->AllocateLocal();
  auto mc = ebbrt::EbbRef<ebbrt::Memcached>(id); 
  mc->Start(MCDPORT);
  ebbrt::kprintf("Memcached server listening on port %d\n", MCDPORT);

  auto id2 = ebbrt::ebb_allocator->AllocateLocal();
  auto tcps = ebbrt::EbbRef<ebbrt::TcpCommand>(id2);
  tcps->Start(5002);
  ebbrt::kprintf("TcpCommand server listening on port %d\n", 5002);
      
  //tpcb.Connect(ebbrt::Ipv4Address({192, 168, 1, 153}), 8888);
  //handler.reset(new TcpSender(std::move(tpcb)));
  //handler->Install();
  
  //handler->SendTest();
  //ebbrt::clock::SleepMilli(1000);
  //handler->SendLog();
  
  /*auto uid = ebbrt::ebb_allocator->AllocateLocal();
  auto udpc = ebbrt::EbbRef<ebbrt::UdpCommand>(uid);
  udpc->Start(6666);
  ebbrt::kprintf("UdpCommand server listening on port %d\n", 6666);*/
}

