#!/usr/bin/python
import socket
import sys
import time
import random
import mutex
import pickle
import Queue as Q
import sequencer
import signal
from threading import Lock, Thread

value_dict = {}
client_requests = []
client_connections = {}
hold_back_queue = Q.PriorityQueue()
output_file_lock = Lock()

def main(argv):
	global sequence_number, output_file
	sequence_number = 0
	global IP, PORT
	IP = "127.0.0.1"
	PORT = 1234
	parsed_file = parse_file(argv[0])
	processes = parsed_file[0]
	global min_delay, max_delay
	min_delay = float(parsed_file[1]/1000)
	max_delay = float(parsed_file[2]/1000)
	found_process = -1

	#CREATE SOCKET TO SEQUENCER
	sequencer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sequencer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sequencer_socket.connect(("127.0.0.1", int(8050)))

	#VISUALIZATION SOCKET
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.sendto("START,SessionNumber1", (IP, PORT))

	for process in processes:
		if(argv[1] in process):
			found_process = process

	output_file = open("output_log" + str(found_process[0]) + ".txt", "w")
	
	try:
		#server and client
		t3 = Thread(target=create_server, args = (sock, min_delay, max_delay, processes, found_process[1], int(found_process[2]), found_process[0], len(processes), sequencer_socket))
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

def signal_handler(signal, frame):
	global output_file
	output_file.close()
	sys.exit(0)

'''
Accepts front-end client connections
'''
def create_server(sock, min_delay, max_delay, processes, host, port, process_id, num_processes, sequencer_socket):
	#print('Creating server for ' + id)

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((host, port))
	s.listen(num_processes)
	client_id = 0
	while True:
		conn, addr = s.accept()
		print("Connected by " +  str(addr))
		client_connections[client_id] = conn
		read_thread = Thread(target = read_server, args= (sock, conn, sequencer_socket, host, port, process_id, client_id))
		read_thread.daemon = True
		read_thread.start()
		client_id += 1
		
	for conn in client_connections:
		conn.close()

#Each thread is for a different process.
def read_server(sock, conn,sequencer_socket, host, port, process_id, client_id):
	global sequence_number, min_delay, max_delay, IP, PORT, output_file
	while True:
		#print("HERE")
		data = conn.recv(1024)
		data_str_split = pickle.loads(data)

		temp_thread = Thread(target=read_message, args = (data_str_split, sock, conn,sequencer_socket, host, port, process_id, client_id))
		temp_thread.daemon = True
		temp_thread.start()

def read_message(data_str_split, sock, conn,sequencer_socket, host, port, process_id, client_id):
	global sequence_number, min_delay, max_delay, IP, PORT, output_file
	#Put Request from front-end client
	#Send this request to sequencer
	print(data_str_split)
	if('sequence_number' not in data_str_split):
		if(data_str_split['method'] == 'put'):
			message_object = {
			'method': "put",
			'var': data_str_split['var'],
			'value': data_str_split['value'],
			'client_num' : data_str_split['client_num'],
			'server_id': process_id,
			'server_host': host,
			'server_port' : port,
			'client_id' : client_id,
			'request_status' : "sent to sequencer"
			}
			data_serialized = pickle.dumps(message_object, -1)
			output_file_lock.acquire()
			output_file.write("SessionNumber1,"+str(message_object['client_id'])+",put,"+str(message_object['var'])+ "," +str(int(time.time())) +",req,"+ str(message_object['value']) + "\n")
			output_file_lock.release()
			sock.sendto("SessionNumber1,"+str(message_object['client_num'])+",put,"+str(message_object['var'])+ "," +str(int(time.time())) +",req,"+ str(message_object['value']), (IP, PORT))
			#client_requests.append((data_str_split, client_id))
			time.sleep(random.randrange(min_delay, max_delay))
			sequencer_socket.sendall(data_serialized)
			#print("Sent put req to sequencer")

		#Get Request from front-end client
		#Send this request to sequencer
		elif(data_str_split['method'] == 'get'):
			message_object = {
			'method': "get",
			'var' : data_str_split['var'],
			'client_num' : data_str_split['client_num'],
			'server_id' : process_id,
			'server_host' : host,
			'server_port' : port,
			'client_id' : client_id,
			'request_status' : "sent to sequencer"
			}
			data_serialized = pickle.dumps(message_object, -1)
			output_file_lock.acquire()
			output_file.write("SessionNumber1,"+str(message_object['client_id'])+",get,"+str(message_object['var'])+","+str(int(time.time())) +",req,\n")
			output_file_lock.release()
			sock.sendto("SessionNumber1,"+str(message_object['client_num'])+",get,"+str(message_object['var'])+","+str(int(time.time())) +",req,", (IP, PORT))
			#client_requests.append((data_str_split), client_id)
			time.sleep(random.randrange(min_delay, max_delay))
			sequencer_socket.sendall(data_serialized)
			#print("Sent get req to sequencer")
		
		#dump request
		elif(data_str_split['method'] == 'dump'):
			client_connections[client_id].sendall("a")
			print(value_dict)

	elif('sequence_number' in data_str_split):
		#Sequencer finished multicasting so tell client that done
		#print(data_str_split['sequence_number'])
		if(data_str_split['request_status'] == "Sequencer finished"):
			if(data_str_split['method'] == "put"):
				output_file_lock.acquire()
				output_file.write("SessionNumber1,"+str(data_str_split['client_id'])+",put,"+str(data_str_split['var'])+","+str(int(time.time())) +",resp,"+ str(data_str_split['value']) + "\n")
				output_file_lock.release()
				sock.sendto("SessionNumber1,"+str(data_str_split['client_num'])+",put,"+str(data_str_split['var'])+","+str(int(time.time())) +",resp,"+ str(data_str_split['value']), (IP, PORT))
				client_connections[data_str_split['client_id']].sendall("a")
			elif(data_str_split['method'] == "get"):
				output_file_lock.acquire()
				output_file.write("SessionNumber1,"+str(data_str_split['client_id'])+",get,"+str(data_str_split['var'])+","+str(int(time.time())) +",resp,"+ str(data_str_split['got_value']) + "\n")
				output_file_lock.release()
				sock.sendto("SessionNumber1,"+str(data_str_split['client_num'])+",get,"+str(data_str_split['var'])+","+str(int(time.time())) +",resp,"+ str(data_str_split['got_value']), (IP, PORT))
				client_connections[data_str_split['client_id']].sendall(str(data_str_split['got_value']))
		
		elif(data_str_split['request_status'] == "multicasting to replicas"):
			if(int(data_str_split['sequence_number']) == (int(sequence_number) + 1)):
				if(data_str_split['method'] == "put"):
					seq = data_str_split['sequence_number']
					#print("Put - " + data_str_split['request_status'] + " - " + str(seq))
					#Update the value in the dictionary for that variable
					variable = data_str_split['var']
					value = data_str_split['value']
					value_dict[variable] = value

					#Send an ack back to sequencer acknowledging variable has been updated for this 
					data_str_split['request_status'] = "Ack to sequencer"
					data_serialized = pickle.dumps(data_str_split, -1)
					time.sleep(random.randrange(min_delay, max_delay))
					sequencer_socket.sendall(data_serialized)
					#print("Put - Sent Ack back to Sequencer - " + str(seq))
				
				elif(data_str_split['method'] == "get"):
					seq = data_str_split['sequence_number']
					#Send an ack back to sequencer acknowledging variable has been updated for this
					#print("Get - " + data_str_split['request_status'] + " - " + str(seq))
					variable = data_str_split['var']
					value = 0
					if(variable in value_dict):
						value = value_dict[variable]
					data_str_split['got_value'] = value
					data_str_split['request_status'] = "Ack to sequencer"
					data_serialized = pickle.dumps(data_str_split, -1)
					time.sleep(random.randrange(min_delay, max_delay))
					sequencer_socket.sendall(data_serialized)
					#print("Get - Sent Ack back to Sequencer - " + str(seq))
				sequence_number = int(sequence_number) + 1
				print(sequence_number)
				if(hold_back_queue.empty() == False):
					temp_thread = Thread(target=checkHoldBackQueue, args = (sequencer_socket, ))
					temp_thread.daemon = True
					temp_thread.start()
			elif(int(data_str_split['sequence_number']) > (int(sequence_number) + 1)):
				print("Putting into hold back queue" + str(data_str_split['sequence_number']))
				hold_back_queue.put((data_str_split['sequence_number'],data_str_split))

'''
Check the hold back queue for items whenever a message from the buffer should be removed
and delivered ("printed") to the screen.
'''
def checkHoldBackQueue(sequencer_socket):
	global sequence_number, min_delay, max_delay
	while True:
		if(hold_back_queue.empty()):
			print("inside here")
			return
		value = hold_back_queue.get(block=True)
		print(value)
		print(value[0])
		print(sequence_number)
		if(int(value[0]) == (int(sequence_number) + 1)):
			data_str_split = value[1]
			if(data_str_split['method'] == "put"):
				#Update the value in the dictionary for that variable
				variable = data_str_split['var']
				value = data_str_split['value']
				value_dict[variable] = value
				seq = data_str_split['sequence_number']
				#Send an ack back to sequencer acknowledging variable has been updated for this 
				data_str_split['request_status'] = "Ack to sequencer"
				data_serialized = pickle.dumps(data_str_split, -1)
				time.sleep(random.randrange(min_delay, max_delay))
				sequencer_socket.sendall(data_serialized)
				print("Sent Ack back to Sequencer - " + str(seq))
				print("Queue size: " + str(hold_back_queue.qsize()))
					
			elif(data_str_split['method'] == "get"):
				#Send an ack back to sequencer acknowledging variable has been updated for this 
				variable = data_str_split['var']
				seq = data_str_split['sequence_number']
				value = 0
				if(variable in value_dict):
					value = value_dict[variable]
				data_str_split['got_value'] = value
				data_str_split['request_status'] = "Ack to sequencer"
				data_serialized = pickle.dumps(data_str_split, -1)
				time.sleep(random.randrange(min_delay, max_delay))
				sequencer_socket.sendall(data_serialized)
				print("Sent Ack back to Sequencer - " + str(seq))
				print("Queue size: " + str(hold_back_queue.qsize()))
		else:
			hold_back_queue.put(value)
			return
		sequence_number += 1
		hold_back_queue.task_done()

if __name__ == "__main__":
	signal.signal(signal.SIGINT, signal_handler)
	main(sys.argv[1:])

