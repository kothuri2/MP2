#!/usr/bin/python
import socket
import threading
import sys, getopt
import datetime
import time
import random
import mutex
import pickle
import Queue as Q
from threading import Lock

client_sockets = []
client_socket_ids = []
vector_timestamps = []

def main(argv):
	parsed_file = parse_file(argv[0])
	processes = parsed_file[0]
	min_delay = float(parsed_file[1]/1000)
	max_delay = float(parsed_file[2]/1000)
	found_process = -1

	for process in processes:
		if(argv[1] in process):
			found_process = process

	for i in range(len(processes)):
		vector_timestamps.append(0)

	try:
		#server and client
		t3 = threading.Thread(target=create_server, args = (found_process[1], int(found_process[2]), found_process[0], len(processes)))
		t3.daemon = True
		t3.start()

		t4 = threading.Thread(target=create_client, args = (found_process, processes, min_delay, max_delay))
		t4.daemon = True
		t4.start()

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
Creates the client for the process and reads in input from the command line.
Depending on whether it is multicast or unicast, the code will adapt accordingly.
'''
def create_client(found_process, processes, min_delay, max_delay):

	while True:
		message = raw_input('')
		message_split = message.split()
		message_to_send_m = ""
		process_id = int(found_process[0])
		multicastFlag = False
		if(message_split[0] == "msend"):
			multicastFlag = True
			for i in range(len(message_split)):
				if(i >= 1):
					message_to_send_m += (message_split[i] + " ")

		if(multicastFlag):
			multicast(message_to_send_m, processes, found_process, min_delay, max_delay)

		else:
			id_to_send = int(message_split[1])
			message_to_send = ""
			for i in range(len(message_split)):
				if(i >= 2):
					message_to_send += (message_split[i] + " ")
			unicast_send(message_to_send, processes, found_process, id_to_send, min_delay, max_delay)

		if(message == 'exit'):
			break

	for s in client_sockets:
		s.close()


'''
Unicast a messsage to only a specific process
'''
def unicast_send(message_to_send, processes, found_process, id_to_send, min_delay, max_delay):

	process_id = int(found_process[0])
	#If the id to send to already exists
	if(int(id_to_send) in client_socket_ids):
		for i in range(len(client_socket_ids)):
			if(int(client_socket_ids[i]) == id_to_send):
				#message = message_to_send + " " + str(found_process[0])
				#simulate random variable network delay
				print("Sent " + message_to_send + "to process " + str(id_to_send) + ", system time is " + str(datetime.datetime.now()))
				temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, process_id, client_sockets[i], process_id, id_to_send))
				temp_thread.daemon = True
				temp_thread.start()
				#time.sleep(random.randrange(min_delay, max_delay))
				#client_sockets[i].sendall(str(message))
	
	#open up a socket to that process and send it
	elif(int(id_to_send) not in client_socket_ids):
		for process in processes:
			if(id_to_send == int(process[0])):
				print('Connecting to server ' + process[0])
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				s.connect((found_process[1], int(process[2])))
				client_sockets.append(s)
				client_socket_ids.append(int(process[0]))
				print("Sent " + message_to_send + "to process " + str(id_to_send) + ", system time is " + str(datetime.datetime.now()))
				temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, process_id, s, process_id, id_to_send))
				temp_thread.daemon = True
				temp_thread.start()

'''
Multicast a message from the current process to all the processes in the group
'''
def multicast(message_to_send, processes, found_process, min_delay, max_delay):
	process_id = int(found_process[0])
	for process in processes:
		if(process[0] != found_process[0] and (int(process[0]) not in client_socket_ids)):
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			try:
				s.connect((found_process[1], int(process[2])))
			except:
				print("unable to connect: other process may not have been started")
				continue
			client_sockets.append(s)
			client_socket_ids.append(int(process[0]))

	vector_timestamps[process_id] += 1
	sequence_number = vector_timestamps[process_id]
	for i in range(len(client_sockets)):
		#simulate random network delay
		print("Sent " + message_to_send + "to process " + str(client_socket_ids[i]) + ", system time is " + str(datetime.datetime.now()))
		temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, sequence_number, client_sockets[i], process_id, client_socket_ids[i]))
		temp_thread.daemon = True
		temp_thread.start()

'''
This method takes in metadata about the process you are and the process you want to send to.
Check if the process that is sending is also the sequencer. If it is the sequencer, then the metadata
is different.
'''
def sendMessage(min_delay, max_delay, message, sequence_number, s, process_id, process_sending_to):

	message_object = {
	'message': str(message),
	'process_id': str(process_id),
	'vector_timestamps': vector_timestamps
	}

	time.sleep(random.randrange(min_delay, max_delay))
	data_serialized = pickle.dumps(message_object, -1)
	s.sendall(data_serialized)
	return

'''
This method sets up the server using SBLA protocol and listens and accepts for other processes
to connect to it. In addition, it spins up a new thread each time a new connection is made so that that
sole thread is responsible for taking in input from that process.
'''
def create_server(host, port, id, num_processes):
	client_connections = []
	#print('Creating server for ' + id)

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((host, port))
	s.listen(num_processes)

	while True:
		conn, addr = s.accept()
		client_connections.append(conn)
		print('Connected by: ',  addr)
		read_thread = threading.Thread(target = read_server, args= (vector_timestamps, conn,) )
		read_thread.start()
		
	for conn in client_connections:
		conn.close()

'''
Each thread in here is responsible for communicating with a different process.
Much of the logic behind Total Ordering is located here.
'''
#Each thread is for a different process.
def read_server(vector_timestamps_server, conn):
	hold_back_queue = Q.PriorityQueue()
	while True:
		data = conn.recv(1024)
		if(not data or str(data) == 'exit'): 
			break

		data_loaded = pickle.loads(data)
		vector_timestamps_client = data_loaded['vector_timestamps']
		message_received = str(data_loaded['message'])
		process_id = int(data_loaded['process_id'])

		print("Received " + str(message_received) + "from process " + str(process_id) + ", system time is " + str(datetime.datetime.now()))


if __name__ == "__main__":
	main(sys.argv[1:])

