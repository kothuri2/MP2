import socket
import sys
import time
import random
import mutex
import pickle
import sys
import Queue as Q
from threading import Lock, Thread

replica_server_connections = []
replica_server_sockets = {}
server_requests = {}
server_sockets_lock = Lock()

def main(config):
	global sequence_number
	sequence_number = 0

	processes, min_delay_t, max_delay_t = parse_file(config)
	global min_delay, max_delay
	min_delay = float(min_delay_t/1000)
	max_delay = float(max_delay_t/1000)
	try:
		#start up sequencer
		t3 = Thread(target=start_up_sequencer, args = ("127.0.0.1", 8050, processes))
		t3.daemon = True
		t3.start()

	except:
		print(sys.exc_info())

	while(True):
		time.sleep(10)

def start_up_sequencer(host, port, processes):

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((host, port))
	s.listen(9)
	
	while True:
		conn, addr = s.accept()
		replica_server_connections.append(conn)
		read_thread = Thread(target = read_server, args= (conn,processes))
		read_thread.daemon = True
		read_thread.start()

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


def read_server(conn, processes):
	global sequence_number, min_delay, max_delay
	while True:
		data = conn.recv(1024)
		data_loaded = pickle.loads(data)
		if(data_loaded['request_status'] == "sent to sequencer"):
			del data_loaded['server_host']
			del data_loaded['server_port']
			sequence_number += 1
			data_loaded['sequence_number'] = sequence_number
			data_loaded['request_status'] = "multicasting to replicas"
			server_requests[sequence_number] = len(replica_server_connections)
			print("GOT request for " + data_loaded['method'] + " " + data_loaded['var'] + " from replica " + data_loaded['server_id'] + "  - seq #: " + str(sequence_number))
			#Multicast Message
			multicast_thread = Thread(target = multicastRequest, args = (data_loaded, processes))
			multicast_thread.daemon = True
			multicast_thread.start()

		#Wait until all Acks from all replicas have been received for this particular request
		#If they all have, send the Sequencer finished ack back to requesting replica server process
		elif(data_loaded['request_status'] == "Ack to sequencer"):
			print("GOT Ack for " + data_loaded['method'] + " " + data_loaded['var'] + "at sequencer")
			req_sequence_number = data_loaded['sequence_number']
			server_requests[req_sequence_number] = server_requests[req_sequence_number] - 1
			if(server_requests[req_sequence_number] == 0):
				print('got all the acks for ' + data_loaded['method'] + " " + data_loaded['var'])
				data_loaded['request_status'] = "Sequencer finished"
				server_id = data_loaded['server_id']
				data_serialized = pickle.dumps(data_loaded, -1)
				time.sleep(random.randrange(min_delay, max_delay))
				replica_server_sockets[server_id].sendall(data_serialized)
				

def create_socket(server_id, host, port):
	#CREATE SOCKET TO REPLICA
	sequencer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sequencer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	try:
		sequencer_socket.connect((host, port))
	except:
		print("Replica Server " + str(server_id) + " has not been started up yet")
		return -1
	server_sockets_lock.acquire()
	replica_server_sockets[server_id] = sequencer_socket
	server_sockets_lock.release()
	return

def multicastRequest(data_loaded, processes):
	#Multicast the message to all replica servers
	method = data_loaded['method']
	variable = data_loaded['var']
	server_id = data_loaded['server_id']
	for process in processes:
		if(process[0] in replica_server_sockets):
			data_serialized = pickle.dumps(data_loaded, -1)
			time.sleep(random.randrange(min_delay, max_delay))
			replica_server_sockets[process[0]].sendall(data_serialized)
			print("Sent multicast to replicas - " + method + " " + variable + " from replica " + server_id)
		else:
			val = create_socket(process[0], process[1], int(process[2]))
			if(val != -1):
				data_serialized = pickle.dumps(data_loaded, -1)
				time.sleep(random.randrange(min_delay, max_delay))
				replica_server_sockets[process[0]].sendall(data_serialized)
				print("Sent multicast to replicas - " + method + " " + variable + " from replica " + server_id)

	return

if __name__ == "__main__":
	main(sys.argv[1])