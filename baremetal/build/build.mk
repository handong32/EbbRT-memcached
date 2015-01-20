MYDIR := $(dir $(lastword $(MAKEFILE_LIST)))

ifeq ($(strip ${EBBRT_SRCDIR}),)
$(error EBBRT_SRCDIR not set)
endif

EBBRT_TARGET := mcd
EBBRT_APP_OBJECTS := mcd.o Memcached.o
EBBRT_APP_VPATH := $(abspath $(MYDIR)../src)
EBBRT_CONFIG := $(abspath $(MYDIR)../src/ebbrtcfg.h)

include $(abspath $(EBBRT_SRCDIR)/apps/ebbrtbaremetal.mk)
