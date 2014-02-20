FILE_PATH := $(dir $(CURDIR)/$(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST)))

EBBRT_APP_VPATH := $(FILE_PATH)/../

EBBRT_CONFIG := $(FILE_PATH)/../config.h

EBBRT_TARGET := app

EBBRT_APP_OBJECTS := \
	app.o Memcached.o

include ../../../../EbbRT/baremetal/build.mk
