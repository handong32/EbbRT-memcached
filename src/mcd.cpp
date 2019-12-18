#include <ebbrt/Debug.h>
#include <ebbrt/EbbAllocator.h>
#include <ebbrt/native/Net.h>
#include "Memcached.h"
#include "UdpCommand.h"

#define MCDPORT 11211

void AppMain()
{
  auto id = ebbrt::ebb_allocator->AllocateLocal();
  auto mc = ebbrt::EbbRef<ebbrt::Memcached>(id);
  mc->Start(MCDPORT);
  ebbrt::kprintf("Memcached server listening on port %d\n", MCDPORT);

  auto uid = ebbrt::ebb_allocator->AllocateLocal();
  auto udpc = ebbrt::EbbRef<ebbrt::UdpCommand>(uid);
  udpc->Start(6666);
  ebbrt::kprintf("UdpCommand server listening on port %d\n", 6666);
}

