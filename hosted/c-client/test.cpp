#include <iostream>
#include <stdexcept>
#include <libmemcached/memcached.h>
#include <libmemcached-1.0/memcached.h>

int main()
{

  //const char *config_string= "--BINARY-PROTOCOL --USE-UDP --SERVER=128.197.41.199";
  const char *config_string= "--BINARY-PROTOCOL --SERVER=128.197.41.199 --USE-UDP";
  //const char *config_string= "--BINARY-PROTOCOL --SERVER=128.197.41.199";
  auto mc= memcached(config_string, strlen(config_string));
  if( mc = nullptr)
    throw std::runtime_error("Memcached creation failed.");

  const char *key= "a";
  const char *value= "1";
 // auto rc =memcached_exist(mc, key, strlen(key));
  auto rc = memcached_set(mc, key, strlen(key), value, strlen(value), 1000, 0);
  std::cout << memcached_last_error_message(mc);
//
//  if (rc == MEMCACHED_INVALID_ARGUMENTS){
//    throw std::runtime_error("Invalid args");
//  }
//  else if (rc == MEMCACHED_NOTFOUND){
//    throw std::runtime_error("Key not found.");
//  }else{
//    std::cout << "Unknown error " << rc <<"\n";
//  }
//
  memcached_servers_reset(mc);
  memcached_free(mc);
}
