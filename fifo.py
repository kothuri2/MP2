#!/usr/bin/python
import socket
import threading
import sys, getopt
import datetime
import time
import random
import mutex
import Queue as Q

def main(argv):
	parsed_file = parse_file(argv[0])
	processes = parsed_file[0]
	min_delay = float(parsed_file[1]/1000)
	max_delay = float(parsed_file[2]/1000)
	found_process = -1

	for process in processes:
		if(argv[1] in process):
			found_process = process

	vector_timestamps = []
	for i in range(len(processes)):
		vector_timestamps.append(0)

	try:
		#server and client
		t3 = threading.Thread(target=create_server, args = (vector_timestamps, found_process[1], int(found_process[2]), found_process[0], len(processes)))
		t3.daemon = True
		t3.start()

		t4 = threading.Thread(target=create_client, args = (vector_timestamps, found_process, processes, min_delay, max_delay))
		t4.daemon = True
		t4.start()

	except:
		print("Unable to start server")

	while(True):
		time.sleep(10)

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

def create_client(vector_timestamps, found_process, processes, min_delay, max_delay):
	client_sockets = []
	client_socket_ids = []
	while True:
		message = raw_input('')
		message_split = message.split()
		message_to_send = ""
		process_id = int(found_process[0])
		multicastFlag = False
		if(message_split[0] == "msend"):
			multicastFlag = True
			for i in range(len(message_split)):
				if(i >= 1):
					message_to_send += (message_split[i] + " ")

		if(multicastFlag == True):
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
				message = message_to_send + " " + str(found_process[0])
				#simulate random network delay
				print("Sent " + message_to_send + "to process " + str(client_socket_ids[i]) + ", system time is " + str(datetime.datetime.now()))
				temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message, sequence_number, client_sockets[i], vector_timestamps))
				temp_thread.daemon = True
				temp_thread.start()
				#time.sleep(random.randrange(min_delay, max_delay))
				#client_sockets[i].sendall(str(message))


		id_to_send = -1
		if(multicastFlag == False):
			id_to_send = int(message_split[1])
			for i in range(len(message_split)):
				if(i >= 2):
					message_to_send += (message_split[i] + " ")

		#If the id to send to already exists
		if(multicastFlag == False and int(id_to_send) in client_socket_ids):
			for i in range(len(client_socket_ids)):
				if(int(client_socket_ids[i]) == id_to_send):
					message = message_to_send + " " + str(found_process[0])
					#simulate random variable network delay
					print("Sent " + message_to_send + "to process " + str(id_to_send) + ", system time is " + str(datetime.datetime.now()))
					temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message, process_id, client_sockets[i], vector_timestamps))
					temp_thread.daemon = True
					temp_thread.start()
					#time.sleep(random.randrange(min_delay, max_delay))
					#client_sockets[i].sendall(str(message))
		
		#open up a socket to that process and send it
		elif(multicastFlag == False and int(id_to_send) not in client_socket_ids):
			for process in processes:
				if(id_to_send == int(process[0])):
					print('Connecting to server ' + process[0])
					s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					s.connect((found_process[1], int(process[2])))
					client_sockets.append(s)
					client_socket_ids.append(int(process[0]))
					message = message_to_send +  " " + str(found_process[0])
					#simulate random network delay
					print("Sent " + message_to_send + "to process " + str(id_to_send) + ", system time is " + str(datetime.datetime.now()))
					temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message, process_id, s, vector_timestamps))
					temp_thread.daemon = True
					temp_thread.start()
					#time.sleep(random.randrange(min_delay, max_delay))
					#s.sendall(str(message))

		if(message == 'exit'):
			break

	for s in client_sockets:
		s.close()

def sendMessage(min_delay, max_delay, message, sequence_number, s, vector_timestamps):
	time.sleep(random.randrange(min_delay, max_delay))
	#increment vector timestamp when you send
	s.sendall(str(message) + " " + str(sequence_number))
	return

def create_server(vector_timestamps, host, port, id, num_processes):
	client_connections = []
	#print('Creating server for ' + id)

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((host, port))
	s.listen(num_processes)

	while True:
		conn, addr = s.accept()
		client_connections.append(conn)
		#print('Connected by: ',  addr)
		read_thread = threading.Thread(target = read_server, args= (vector_timestamps, conn,) )
		read_thread.start()
		
	for conn in client_connections:
		conn.close()

#Each thread is for a different process.
def read_server(vector_timestamps, conn):
	hold_back_queue = Q.PriorityQueue()
	while True:
		data = conn.recv(1024)
		if(not data or str(data) == 'exit'): 
			break

		message_split = str(data).split()
		id_received = int(message_split[len(message_split) - 2])
		sequence_number = int(message_split[len(message_split) - 1])
		message_received = ""
		for i in range(len(message_split) - 2):
				message_received += (message_split[i] + " ")

		#print("id_received : " + str(id_received) + " sequence_number: "+ str(sequence_number) + " vector_timestamps[id_received]: " + str(vector_timestamps[id_received]))
		if(sequence_number == vector_timestamps[id_received] + 1):
			print("Received " + str(message_received) + "from process " + str(id_received) + ", system time is " + str(datetime.datetime.now()))
			vector_timestamps[id_received] = sequence_number
			if(hold_back_queue.empty() == False):
				temp_thread = threading.Thread(target=checkHoldBackQueue, args = (hold_back_queue, vector_timestamps, id_received))
				temp_thread.daemon = True
				temp_thread.start()
				
		elif(sequence_number > (vector_timestamps[id_received] + 1)):
			#print("Placing into hold back queue")
			#print("sequence_number: " + str(sequence_number) + " id_received: " + str(id_received) + " message_received: " + message_received)
			hold_back_queue.put((sequence_number, id_received, message_received))

def checkHoldBackQueue(hold_back_queue, vector_timestamps, id_received):
	while True:
		if(hold_back_queue.empty()):
			return
		#print("Checked from hold_back_queue")
		value = hold_back_queue.get(block=True)
		#print("Removed from hold_back_queue")
		#print(value)
		if(value[1] == id_received and value[0] == (vector_timestamps[id_received] + 1) ):
			print("Received " + str(value[2]) + "from process " + str(id_received) + ", system time is " + str(datetime.datetime.now()))
		else:
			hold_back_queue.put(value)
			return
		vector_timestamps[id_received] = value[0]
		hold_back_queue.task_done()
			

if __name__ == "__main__":
	if(len(sys.argv) != 3):
        print('Usage: python %s <config file name> <process number>' % sys.argv[0]) #usage
        exit(1)
	main(sys.argv[1:])

