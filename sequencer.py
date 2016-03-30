import socket
import sys
import time
import random
import mutex
import pickle
import Queue as Q
from threading import Lock, Thread

def main(argv):
	
def start_up_sequencer():

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((127.0.0.1, 6000))
	s.listen()
	
	while True:
		conn, addr = s.accept()
		client_connections.append((conn, client_id))
		#print('Connected by: ',  addr)
		read_thread = Thread(target = read_server, args= (conn, client_id))
		read_thread.daemon = True
		read_thread.start()
		client_id += 1

def read_server():


if __name__ == "__main__":
	main(sys.argv[1:])