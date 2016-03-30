import socket
import sys
import time
from threading import Thread, Lock

'''
1. connect with the other servers/clients
2. listen for incoming connections
3. 
'''

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python %s <config file name> <server pair id>' % sys.argv[0]) #usage
        exit(1)
    main(sys.argv[1], sys.argv[2])