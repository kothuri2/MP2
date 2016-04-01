import socket
import signal
import sys
import time
import random
import mutex
import pickle
import Queue as Q
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
client_sockets = {}

def main(config, server_id, W, R):
    global found_process
    global processes
    parsed_file = parse_file(config)
    processes = parsed_file[0]
    min_delay = float(parsed_file[1]/1000)
    max_delay = float(parsed_file[2]/1000)
    found_process = -1

    for process in processes:
        if(server_id in process):
            found_process = process

    try:
        #server and client
        server_thread = Thread(target=create_server, args = (int(W), int(R), min_delay, max_delay, processes, found_process[1], int(found_process[2]), found_process[0], len(processes)))
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
def create_server(W, R, min_delay, max_delay, processes, host, port, process_id, num_processes):
    global client_connections
    client_connections = {}

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(num_processes)
    client_id = 0
    global start
    start = 0
    while True:
        conn, addr = s.accept()
        client_connections[client_id] = conn
        #print("client_id: " + (str)(client_id))
        read_thread = Thread(target = read_server, args= (min_delay, max_delay, W, R, conn, client_id, process_id, client_connections))
        read_thread.daemon = True
        read_thread.start()
        client_id += 1
        
    for conn in client_connections:
        conn.close()


#Each thread is for a different process.
def read_server(min_delay, max_delay, W, R, conn, client_id, server_id, client_connections):
    global start
    global processes
    while True:
        ignore = 0
        #print "start: " + str(start)
        data = conn.recv(1024)
        #print "read"
        #print(not data)
        #print (data == '')
        if(not data):
            ignore = 1
        if(start == 0):
            create_connections()
            start = 1
        #print ignore
        #print data
        if(ignore == 0):
            data_str_split = pickle.loads(data)

        #print data_str_split
        
        data_serialized = -1
        #Put Request from front-end client
        #Send this request to sequencer 
        if(ignore == 0 and 'from_server' not in data_str_split):
            if(data_str_split['method'] == 'put'):
                if(data_str_split['var'] in value_dict):
                    #print data_str_split['value']
                    data_str_split['value'] = [data_str_split['value'], value_dict[data_str_split['var']][1] + 1, server_id]
                    #print data_str_split['value']
                else:
                    data_str_split['value'] = [data_str_split['value'], 1, server_id] # initialize time stamp to 1
                message_object = {
                'from_server' : True,
                'method': "put",
                'var' : data_str_split['var'],
                'value' : data_str_split['value'],
                'server_id' : server_id
                }
                data_serialized = pickle.dumps(message_object, -1)
                client_requests.append([(data_str_split), client_id, W])
                if(W == 0):
                    #print "this?"
                    value_dict[data_str_split['var']] = data_str_split['value']
                    client_connections[client_id].sendall("a")

            #Get Request from front-end client
            #Send this request to sequencer
            elif(data_str_split['method'] == 'get'):
                message_object = {
                'from_server' : True,
                'method': "get",
                'var' : data_str_split['var'],
                'server_id' : server_id
                }
                data_serialized = pickle.dumps(message_object, -1)
                client_requests.append([(data_str_split), client_id, R])
                if(R == 0):
                    if data_str_split['var'] in value_dict:
                        #print value_dict[data_str_split['var']]
                        client_connections[client_id].sendall(value_dict[data_str_split['var']][0])
                    else:
                        client_connections[client_id].sendall('-1')
            
            #dump request
            elif(data_str_split['method'] == 'dump'):
                print(value_dict)
                client_connections[client_id].sendall("a")

        if(data_serialized != -1):
            for socket in client_sockets:
                send_thread = Thread(target=send_message, args = (min_delay, max_delay, data_serialized, client_sockets[socket]))
                send_thread.daemon = True
                send_thread.start()

        if(ignore == 0 and 'from_server' in data_str_split and data_str_split['from_server']):
            #print "RECEIVED FROM SERVER"
            if('acknowledgement' in data_str_split):
                for request in client_requests:
                    if(request[0]['var'] == data_str_split['var'] and request[0]['method'] == data_str_split['method']):
                        #print request[2]
                        request[2] -= 1 #decrement W/R
                        if(data_str_split['method'] == "get" or data_str_split['method'] == "put"):
                            #print data_str_split
                            #print request[0]
                            if('value' not in request[0] or data_str_split['other_vals'][1] > request[0]['value'][1]):
                                request[0]['value'] = data_str_split['other_vals']
                            elif(data_str_split['other_vals'][1] == request[0]['value'][1] and data_str_split['other_vals'][2] > request[0]['value'][2]):
                                request[0]['value'] = data_str_split['other_vals']
                            elif(request[0]['var'] in value_dict):
                                if(data_str_split['other_vals'][1] == value_dict[request[0]['var']][1] and data_str_split['other_vals'][2] < value_dict[request[0]['var']][2]):
                                    request[0]['value'] = value_dict[request[0]['var']]
                        if(request[2] == 0):
                            if(data_str_split['method'] == "put"):
                                #print value_dict
                                #print request[0]['var']
                                #print data_str_split['other_vals']
                                #print request[0]['value']
                                #print value_dict[request[0]['var']]
                                value_dict[request[0]['var']] = request[0]['value']
                                #print value_dict[request[0]['var']]
                                client_connections[request[1]].sendall("a") # send the client acknowledgement of put finishing
                            elif(data_str_split['method'] == "get"):
                                client_connections[request[1]].sendall(request[0]['value'][0]) # send the client the most recent value
                            client_requests.remove(request)
            
            elif(data_str_split['method'] == "put"):
                if(data_str_split['var'] not in value_dict or data_str_split['value'][1] > value_dict[data_str_split['var']][1]):
                    value_dict[data_str_split['var']] = data_str_split['value'] # modify value and timestamp in dict if not existent or timestamp is greater
                elif(data_str_split['value'][1] == value_dict[data_str_split['var']][1] and data_str_split['server_id'] > value_dict[data_str_split['var']][2]):
                    value_dict[data_str_split['var']] = data_str_split['value'] # modify value and timestamp in dict if timestampm is equal and server id is greater
                data_str_split['other_server'] = server_id
                if(data_str_split['var'] in value_dict):
                    data_str_split['other_vals'] = value_dict[data_str_split['var']] # add value and timestamp tuple
                else:
                    data_str_split['other_vals'] = ['-1', -1]
                data_str_split['acknowledgement'] = True
                data_serialized = pickle.dumps(data_str_split, -1)
                time.sleep(random.randrange(min_delay, max_delay))
                client_sockets[data_str_split['server_id']].sendall(data_serialized)

            elif(data_str_split['method'] == "get"):
                if(data_str_split['var'] in value_dict):
                    data_str_split['other_vals'] = value_dict[data_str_split['var']] # add value and timestamp tuple
                else:
                    data_str_split['other_vals'] = ['-1', -1]
                data_str_split['other_server'] = server_id
                data_str_split['acknowledgement'] = True
                data_serialized = pickle.dumps(data_str_split, -1)
                time.sleep(random.randrange(min_delay, max_delay))
                client_sockets[data_str_split['server_id']].sendall(data_serialized)
                
        #print "end"

def send_message(min_delay, max_delay, data_serialized, socket):
    time.sleep(random.randrange(min_delay, max_delay))
    socket.sendall(data_serialized)

def create_connections():
    #print "enter"
    global found_process
    client_socket_ids = []
    for process in processes:
        if(process[0] != found_process[0] and (int(process[0]) not in client_socket_ids)):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect((found_process[1], int(process[2])))
            except:
                print("unable to connect: other process may not have been started")
                continue
            client_sockets[process[0]] = s
            client_socket_ids.append(int(process[0]))
    #print "exit"

def signal_handler(signal, frame):
    for client in client_connections:
        client_connections[client].sendall("Q")
    sys.exit(0)

if __name__ == "__main__":
    if(len(sys.argv) != 5):
        print('Usage: python %s <config file name> <server id> <W> <R>' % sys.argv[0]) #usage
        exit(1)
    signal.signal(signal.SIGINT, signal_handler)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
