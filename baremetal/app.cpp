#include <ebbrt/Debug.h>
#include <ebbrt/EbbAllocator.h>
#include <ebbrt/Net.h>
#include <ebbrt/Trace.h>
#include "Memcached.h"

void AppMain()
{
  auto id = ebbrt::ebb_allocator->AllocateLocal();
  auto mc = ebbrt::EbbRef<ebbrt::Memcached>(id);
  mc->StartListening(11211);
  ebbrt::kprintf("Memcached server listening on 11211\n");
#if __EBBRT_ENABLE_TRACE__
  ebbrt::trace::Init();
#endif
}

