//          Copyright Boston University SESA Group 2013 - 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
#include <cstdlib>
#include <sstream>
#include <string>
#include <ebbrt/SharedIOBufRef.h>
#include <ebbrt/UniqueIOBuf.h>
#include <ebbrt/native/Net.h>
#include <ebbrt/native/Msr.h>

#include "UdpCommand.h"


// Vol. 3C Page 35-3, Table 35-2. IA-32 Architectural MSRs
#define IA32_APIC_BASE 0x1B
#define IA32_FEATURE_CONTROL 0x3A
#define IA32_SMM_MONITOR_CTL 0x9B
#define IA32_MTRRCAP 0xFE
#define IA32_SYSENTER_CS 0x174
#define IA32_MCG_CAP 0x179
#define IA32_PERF_STATUS 0x198
#define IA32_PERF_CTL    0x199
#define IA32_CLOCK_MODULATION 0x19A
#define IA32_THERM_INTERRUPT 0x19B
#define IA32_THERM_STATUS 0x19C
#define IA32_MISC_ENABLE 0x1A0
#define IA32_PACKAGE_THERM_STATUS 0x1B1
#define IA32_PACKAGE_THERM_INTERRUPT 0x1B2
#define IA32_PLATFORM_DCA_CAP 0x1F8
#define IA32_CPU_DCA_CAP 0x1F9
#define IA32_DCA_0_CAP 0x1FA

// Vol. 3C Page 35-143, Table 35-18. Intel Sandy Bridge MSRs
#define MSR_PLATFORM_INFO 0xCE
#define MSR_PKG_CST_CONFIG_CONTROL 0xE2
#define MSR_PMG_IO_CAPTURE_BASE 0xE4
#define MSR_TEMPERATURE_TARGET 0x1A2
#define MSR_MISC_FEATURE_CONTROL 0x1A4
#define MSR_PEBS_LD_LAT 0x3F6
#define MSR_PKG_C3_RESIDENCY 0x3F8
#define MSR_PKG_C6_RESIDENCY 0x3F9

// TODO
#define MSR_PKGC3_IRTL 0x60A
#define MSR_PKGC6_IRTL 0x60B


ebbrt::UdpCommand::UdpCommand() {}

void ebbrt::UdpCommand::Start(uint16_t port) {
  udp_pcb.Bind(port);
  udp_pcb.Receive([this](Ipv4Address from_addr, uint16_t from_port,
			 std::unique_ptr<MutIOBuf> buf) {
		    ReceiveCommand(from_addr, from_port, std::move(buf));
		  });
  uint32_t mcore = static_cast<uint32_t>(Cpu::GetMine());
  ebbrt::kprintf_force("Core: %u %s\n", mcore, __PRETTY_FUNCTION__);
}

void ebbrt::UdpCommand::ReceiveCommand(
    Ipv4Address from_addr, uint16_t from_port, std::unique_ptr<MutIOBuf> buf) {
  //uint32_t mcore = static_cast<uint32_t>(Cpu::GetMine());
  std::string s(reinterpret_cast<const char*>(buf->Data()));
  std::string delimiter = ",";
  uint32_t pos = 0, param = 0;
  std::string token1, token2;

  pos = s.find(delimiter);
  token1 = s.substr(0, pos);
  token2 = s.substr(pos+1, s.length());
  param = static_cast<uint32_t>(atoi(token2.c_str()));

  if(token1 == "cpu_config_write") {
    for (uint32_t i = 0; i < static_cast<uint32_t>(Cpu::Count()); i++) {
      event_manager->SpawnRemote(
	[this, i] () mutable {
	  // disables turbo boost
	  ebbrt::msr::Write(IA32_MISC_ENABLE, 0x4000850089);
	  // same p state as Linux with performance governor
	  ebbrt::msr::Write(IA32_PERF_CTL, 0x1D00);
	  ebbrt::kprintf("Core %u: cpu_config_write done\n", i);
	}, i);
    }
  }
  else if(token1 == "cpu_config_read") {
    for (uint32_t i = 0; i < static_cast<uint32_t>(Cpu::Count()); i++) {
      event_manager->SpawnRemote(
	[this, i] () mutable {
	  //uint64_t tmp = ebbrt::msr::Read(IA32_APIC_BASE);
	  //ebbrt::kprintf_force("Core %u: IA32_APIC_BASE(0x%X) = 0x%llX\n", i, IA32_APIC_BASE, tmp);

	  uint64_t tmp = ebbrt::msr::Read(IA32_FEATURE_CONTROL);
	  ebbrt::kprintf_force("Core %u: IA32_FEATURE_CONTROL(0x%X) = 0x%llX\n", i, IA32_FEATURE_CONTROL, tmp);

	  //tmp = ebbrt::msr::Read(IA32_SMM_MONITOR_CTL);
	  //ebbrt::kprintf_force("Core %u: IA32_SMM_MONITOR_CTL(0x%X) = 0x%llX\n", i, IA32_SMM_MONITOR_CTL, tmp);

	  tmp = ebbrt::msr::Read(IA32_MTRRCAP);
	  ebbrt::kprintf_force("Core %u: IA32_MTRRCAP(0x%X) = 0x%llX\n", i, IA32_MTRRCAP, tmp);

	  //tmp = ebbrt::msr::Read(IA32_SYSENTER_CS);
	  //ebbrt::kprintf_force("Core %u: IA32_SYSENTER_CS(0x%X) = 0x%llX\n", i, IA32_SYSENTER_CS, tmp);

	  tmp = ebbrt::msr::Read(IA32_MCG_CAP);
	  ebbrt::kprintf_force("Core %u: IA32_MCG_CAP(0x%X) = 0x%llX\n", i, IA32_MCG_CAP, tmp);

	  tmp = ebbrt::msr::Read(IA32_PERF_STATUS);
	  ebbrt::kprintf_force("Core %u: IA32_PERF_STATUS(0x%X) = 0x%llX\n", i, IA32_PERF_STATUS, tmp);
	  
	  tmp = ebbrt::msr::Read(IA32_PERF_CTL);
	  ebbrt::kprintf_force("Core %u: IA32_PERF_CTL(0x%X) = 0x%llX\n", i, IA32_PERF_CTL, tmp);

	  tmp = ebbrt::msr::Read(IA32_CLOCK_MODULATION);
	  ebbrt::kprintf_force("Core %u: IA32_CLOCK_MODULATION(0x%X) = 0x%llX\n", i, IA32_CLOCK_MODULATION, tmp);

	  tmp = ebbrt::msr::Read(IA32_THERM_INTERRUPT);
	  ebbrt::kprintf_force("Core %u: IA32_THERM_INTERRUPT(0x%X) = 0x%llX\n", i, IA32_THERM_INTERRUPT, tmp);

	  tmp = ebbrt::msr::Read(IA32_THERM_STATUS);
	  ebbrt::kprintf_force("Core %u: IA32_THERM_STATUS(0x%X) = 0x%llX\n", i, IA32_THERM_STATUS, tmp);

	  tmp = ebbrt::msr::Read(IA32_MISC_ENABLE);
	  ebbrt::kprintf_force("Core %u: IA32_MISC_ENABLE(0x%X) = 0x%llX\n", i, IA32_MISC_ENABLE, tmp);

	  tmp = ebbrt::msr::Read(IA32_PACKAGE_THERM_INTERRUPT);
	  ebbrt::kprintf_force("Core %u: IA32_PACKAGE_THERM_INTERRUPT(0x%X) = 0x%llX\n", i, IA32_PACKAGE_THERM_INTERRUPT, tmp);

	  tmp = ebbrt::msr::Read(IA32_PACKAGE_THERM_STATUS);
	  ebbrt::kprintf_force("Core %u: IA32_PACKAGE_THERM_STATUS(0x%X) = 0x%llX\n", i, IA32_PACKAGE_THERM_STATUS, tmp);
	  
	  tmp = ebbrt::msr::Read(IA32_PLATFORM_DCA_CAP);
	  ebbrt::kprintf_force("Core %u: IA32_PLATFORM_DCA_CAP(0x%X) = 0x%llX\n", i, IA32_PLATFORM_DCA_CAP, tmp);

	  tmp = ebbrt::msr::Read(IA32_CPU_DCA_CAP);
	  ebbrt::kprintf_force("Core %u: IA32_CPU_DCA_CAP(0x%X) = 0x%llX\n", i, IA32_CPU_DCA_CAP, tmp);
	  
	  tmp = ebbrt::msr::Read(IA32_DCA_0_CAP);
	  ebbrt::kprintf_force("Core %u: IA32_DCA_0_CAP(0x%X) = 0x%llX\n", i, IA32_DCA_0_CAP, tmp);

	  // sandy bridge only
	  tmp = ebbrt::msr::Read(MSR_PLATFORM_INFO);
	  ebbrt::kprintf_force("Core %u: MSR_PLATFORM_INFO(0x%X) = 0x%llX\n", i, MSR_PLATFORM_INFO, tmp);

	  tmp = ebbrt::msr::Read(MSR_PKG_CST_CONFIG_CONTROL);
	  ebbrt::kprintf_force("Core %u: MSR_PKG_CST_CONFIG_CONTROL(0x%X) = 0x%llX\n", i, MSR_PKG_CST_CONFIG_CONTROL, tmp);

	  tmp = ebbrt::msr::Read(MSR_PMG_IO_CAPTURE_BASE);
	  ebbrt::kprintf_force("Core %u: MSR_PMG_IO_CAPTURE_BASE(0x%X) = 0x%llX\n", i, MSR_PMG_IO_CAPTURE_BASE, tmp);

	  tmp = ebbrt::msr::Read(MSR_TEMPERATURE_TARGET);
	  ebbrt::kprintf_force("Core %u: MSR_TEMPERATURE_TARGET(0x%X) = 0x%llX\n", i, MSR_TEMPERATURE_TARGET, tmp);

	  tmp = ebbrt::msr::Read(MSR_MISC_FEATURE_CONTROL);
	  ebbrt::kprintf_force("Core %u: MSR_MISC_FEATURE_CONTROL(0x%X) = 0x%llX\n", i, MSR_MISC_FEATURE_CONTROL, tmp);
	  
	}, i);
    }
  } else {
    for (uint32_t i = 0; i < static_cast<uint32_t>(Cpu::Count()); i++) {
      event_manager->SpawnRemote(
	[this, token1, param, i] () mutable {
	  ebbrt::kprintf_force("SpawnRemote %u\n", i);
	  network_manager->Config(token1, param);
	}, i);
    }
  }
  //ebbrt::kprintf_force("Core: %u ReceiveCommand() from_port=%u message:%s\n", mcore, from_port, s.c_str());
}
