# EbbRT-memcached
EbbRT based memcached application

## Requirements
* (Optional) Set the `EBBRT_SRCDIR` environment variable to point to 
EbbRT source directory: `export EBBRT_SRCDIR=~/EbbRT`
* Build and install EbbRT native toolchain
* Build and install EbbRT hosted library
* Build and install [EbRRT cmdline library](https://github.com/SESA/EbbRT/tree/master/libs/cmdline)

## Build

Set the `EBBRT_SYSROOT` environment variable to point to EbbRT native toolchain, assuming 
it was installed at following location:

`export EBBRT_SYSROOT=~/sysroot/native`

Set the `CMAKE_PREFIX_PATH` environment variable to point to EbbRT hosted library, assuming 
it was installed at following location:

`export CMAKE_PREFIX_PATH=~/sysroot/hosted`

To build native and hosted memcached application:

`make -j all`

To run:

`./build/Memcached`

