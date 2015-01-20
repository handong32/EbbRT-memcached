import sys
import pylibmc

try: 
  ip = sys.argv[1]
except IndexError:
  ip = None

if ip is None:
    print "Error: need server ip"
    exit(1)

raw_input("Press a key connect... ") 
mc = pylibmc.Client([ip], binary=True)

while True:
  raw =str(raw_input("$ "))
  split = raw.split()
  cmd = [x.replace(' ', '') for x in split]
# get
  if cmd[0] == 'get' and cmd[1] != None:
    print mc.get(str(cmd[1]))

# get multi
# set multi
# replace 
# append
# prepend
# incr
# decr
# delete
# delte_multi
# disconnect_all
# flush_all

# add
  elif cmd[0] == 'add' and cmd[1] != None:
    print mc.add(str(cmd[1]))
# set
  elif cmd[0] == 'set' and cmd[1] != None and cmd[2] != None:
    mc.set(cmd[1], cmd[2])
# quit
  elif cmd[0] == 'quit':
    print mc.disconnect_all()
# stats
  elif cmd[0] == 'stats':
    print mc.get_stats()
  else:
    print "input error:", cmd
