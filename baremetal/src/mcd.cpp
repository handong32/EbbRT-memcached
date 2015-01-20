#include <ebbrt/Debug.h>
#include <ebbrt/EbbAllocator.h>
#include <ebbrt/Net.h>
#include <ebbrt/Trace.h>
#include "Memcached.h"

#define MCDPORT 11211

void AppMain()
{
  auto id = ebbrt::ebb_allocator->AllocateLocal();
  auto mc = ebbrt::EbbRef<ebbrt::Memcached>(id);
  mc->Start(MCDPORT);
  ebbrt::kprintf("Memcached server listening on port %d\n", MCDPORT);
}

