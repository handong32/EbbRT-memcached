import sys
import os
import pylibmc
ip = sys.argv[1]
its = int(sys.argv[2])

if ip is None:
    print "Error: need server ip"
    exit(1)

#raw_input("Press a key connect... ") 
mc = pylibmc.Client([ip], binary=True )

#raw_input("Press a key start slapping... ") 

while (its > 0): 
    mc.set(os.urandom(100), os.urandom(400))
    its = its - 1
