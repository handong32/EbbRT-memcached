#include <ebbrt/Debug.h>
#include <ebbrt/EbbAllocator.h>
#include <ebbrt/Net.h>
#include "Memcached.h"

void appmain()
{
  auto id = ebbrt::ebb_allocator->AllocateLocal();
  auto mc = ebbrt::EbbRef<ebbrt::Memcached>(id);
  mc->StartListening(11211);
}

