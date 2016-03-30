#!/usr/bin/python
import socket
import sys
import time
import random
import mutex
import pickle
import Queue as Q
import sequencer
from threading import Lock, Thread

value_dict = {}
client_requests = []

def main(argv):
	parsed_file = parse_file(argv[0])
	processes = parsed_file[0]
	min_delay = float(parsed_file[1]/1000)
	max_delay = float(parsed_file[2]/1000)
	found_process = -1

	#CREATE SOCKET TO SEQUENCER
	sequencer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sequencer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sequencer_socket.connect((127.0.0.1, int(6000)))

	for process in processes:
		if(argv[1] in process):
			found_process = process

	for i in range(len(processes)):
		vector_timestamps.append(0)

	try:
		#server and client
		t3 = Thread(target=create_server, args = (min_delay, max_delay, processes, found_process[1], int(found_process[2]), found_process[0], len(processes), sequencer_socket))
		t3.daemon = True
		t3.start()

	except:
		print("Unable to start server")

	while(True):
		time.sleep(10)

'''
Parse the config file for the data about the processes and the min_delay, max_delay.
'''
def parse_file(file_name):
	processes = []
	min_delay = ""
	max_delay = ""
	with open(file_name) as f:
		for line in f:
			if(len(line.split()) != 0):
				cur_process = line.split()
				if(len(cur_process) == 2):
					min_delay = cur_process[0]
					max_delay = cur_process[1]
				else:
					processes.append(cur_process)

	return (processes, int(min_delay), int(max_delay))

'''
Accepts front-end client connections
'''
def create_server(min_delay, max_delay, processes, host, port, process_id, num_processes, sequencer_socket):
	client_connections = []
	#print('Creating server for ' + id)

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((host, port))
	s.listen(num_processes)
	client_id = 0
	while True:
		conn, addr = s.accept()
		client_connections.append((conn, client_id))
		#print('Connected by: ',  addr)
		read_thread = Thread(target = read_server, args= (conn, client_id, sequencer_socket))
		read_thread.daemon = True
		read_thread.start()
		client_id += 1
		
	for conn in client_connections:
		conn.close()

#Each thread is for a different process.
def read_server(conn, client_id, sequencer_socket):
	while True:
		data = conn.recv(1024)

		data_str_split = pickle.loads(data)
		
		#Put Request from front-end client
		#Send this request to sequencer 
		if(data_str_split['method'] == 'put'):
			message_object = {
			'method': "put"
			'var' : data_str_split['var'],
			'value' : data_str_split['value'],
			'client_id' : client_id
			}
			data_serialized = pickle.dumps(message_object, -1)
			sequencer_socket.sendall(data_serialized)

		#Get Request from front-end client
		#Send this request to sequencer
		elif(data_str_split['method'] == 'get'):
			message_object = {
			'method': "get"
			'var' : data_str_split['var'],
			'client_id' : client_id
			}
			data_serialized = pickle.dumps(message_object, -1)
			sequencer_socket.sendall(data_serialized)
		#dump request
		elif(data_str_split['method'] == 'dump'):
			print(value_dict)

if __name__ == "__main__":
	main(sys.argv[1:])

