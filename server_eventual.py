import socket
import sys
import time
import random
import mutex
import pickle
import Queue as Q
import sequencer
from threading import Lock, Thread

'''
EVENTUAL CONSISTENCY (from the view of a server):
client connects to server replica (done)
server replica creates a thread to listen on from that client and gives that thread an id (done)
user inputs put x 3 (done)
client 1 sends px3 to server replica, id = 2 (done)
client 1 blocks waiting for acknowledgement (done)
server replica receieves px3 and stores a tuple of 1px3 and the value of W in a list (done)
server replica updates the value of x and increments x's timestamp by one
server replica multicasts a pickled object of px3, the timestamp, and the server process id
other servers receive the multicast and compare the timestamp in the pickle with their timestamp for x
    if the pickled timestamp is greater, change x and update the timestamp
    if the pickled timestamp is less, don't change x or the timestamp
    if the pickled timestamp is equal and the pickled server process is greater, change the x and update the timestamp
    if the pickled timestamp is equal and the pickled server process is less, don't change x or the timestamp
other servers send the acknowledgement back
server replica decrements W for 1px3 until the value is 0, at which point it removes it from the list
server replica sends client an acknlowedgement
client prints "Acknowledged" (done)
client opens up for next input (done)


DEAL WITH CRASHED SERVERS 
'''
value_dict = {}
client_requests = []
client_sockets = []

def main(argv):
    global found_process
    global processes
    parsed_file = parse_file(argv[0])
    processes = parsed_file[0]
    min_delay = float(parsed_file[1]/1000)
    max_delay = float(parsed_file[2]/1000)
    found_process = -1

    for process in processes:
        if(argv[1] in process):
            found_process = process

    try:
        #server and client
        server_thread = Thread(target=create_server, args = (min_delay, max_delay, processes, found_process[1], int(found_process[2]), found_process[0], len(processes)))
        server_thread.daemon = True
        server_thread.start()

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
def create_server(min_delay, max_delay, processes, host, port, process_id, num_processes):
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
        read_thread = Thread(target = read_server, args= (conn, client_id, process_id))
        read_thread.daemon = True
        read_thread.start()
        client_id += 1
        
    for conn in client_connections:
        conn.close()


#Each thread is for a different process.
def read_server(conn, client_id, server_id):
    start = 0
    while True:
        data = conn.recv(1024)

        if(start == 0):
            create_connections()
            start = 1

        data_str_split = pickle.loads(data)
        
        data_serialized = -1
        #Put Request from front-end client
        #Send this request to sequencer 
        if(data_str_split['method'] == 'put'):
            message_object = {
            'method': "put"
            'var' : data_str_split['var'],
            'value' : data_str_split['value'],
            'client_id' : server_id
            }
            data_serialized = pickle.dumps(message_object, -1)
            client_requests.append(((data_str_split), client_id, W))

        #Get Request from front-end client
        #Send this request to sequencer
        elif(data_str_split['method'] == 'get'):
            message_object = {
            'method': "get"
            'var' : data_str_split['var'],
            'client_id' : client_id
            }
            data_serialized = pickle.dumps(message_object, -1)
            client_requests.append(((data_str_split), client_id, R))
            
        
        #dump request
        elif(data_str_split['method'] == 'dump'):
            print(value_dict)

        if(data_serialized != -1):
            for socket in client_sockets:
                socket.sendall(data_serialized)



def create_connections():
    global found_process
    global processes
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

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python %s <config file name> <server pair id>' % sys.argv[0]) #usage
        exit(1)
    main(sys.argv[1], sys.argv[2])