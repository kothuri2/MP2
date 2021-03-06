import socket
import sys
import time
import pickle
from threading import Thread, Lock

go = 1

def main(config, server_id, client_id):

    server = parse_file(config, server_id).split()

    client_thread = Thread(target=create_client, args=(server[1], server[2], client_id, server_id)) #start client
    client_thread.daemon = True
    client_thread.start()

    while True:
        time.sleep(1) #keep main function running

def parse_file(file_name, server_id):
    global processes
    i = 0
    server = ''
    processes = []
    with open(file_name) as f:
        for line in f:
            if(i < 1):
                i += 1
            elif(len(line.split()) != 0):
                processes.append(line.split())
                if(line.split()[0] == server_id):
                    server = line
    return server
'''
Creates the client for the process and reads in input from the command line.
Depending on whether it is multicast or unicast, the code will adapt accordingly.
'''
def create_client(ip, port, client_id, server_id):
    global s
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect((ip, int(port)))
    except:
        print("unable to connect: other process may not have been started")
        exit(1)

    read_client_thread = Thread(target = read_client, args=(client_id))
    read_client_thread.daemon = True
    read_client_thread.start()

    read_server_thread = Thread(target = read_server, args=(server_id))
    read_server_thread.daemon = True
    read_server_thread.start()

def read_client(client_id):
    global s
    global go
    while True:
        #print "before"
        message = raw_input('')
        #print "message: " + message 
        #print "After"
        
        while go == 0: # Wait until the previous request is acknowledged
            time.sleep(1)
        message_split = message.split()
        data_serialized = -1
        if(message_split[0] == "get"):
            message_object = {
                'method' : "get",
                'var' : message_split[1],
                'client_num' : client_id
            }
            data_serialized = pickle.dumps(message_object, -1)

        elif(message_split[0] == "put"):
            message_object = {
                'method' : "put",
                'var' : message_split[1],
                'value' : message_split[2],
                'client_num' : client_id
            }
            data_serialized = pickle.dumps(message_object, -1)

        elif(message_split[0] == "dump"):
            message_object = {
                'method' : "dump",
            }
            data_serialized = pickle.dumps(message_object, -1)

        elif(message_split[0] == "delay"):
            time.sleep(float(message_split[1])/1000.0)

        else:
            print "invalid message"

        if(data_serialized != -1):
            s.sendall(data_serialized)
            go = 0
        #print "done"
def read_server(server_id):
    server_id = (int)(server_id)
    global s
    global go
    global processes
    while True:
        try:
            buffer = s.recv(128)
        except:
            break
        if(buffer == "a"):
            print "Acknowledged" # Acknowledges dumps and puts
        elif(buffer == "Q"):
            s = None
            while s == None and processes:
                print processes
                print server_id
                if(int(processes[len(processes)-1][0]) == int(server_id)):
                    server_id = 0
                    process = processes[0]
                else:
                    for i in range(len(processes)):
                        if((int)(processes[i][0]) == (int)(server_id) + 1):
                            process = processes[i]
                            break
                    server_id += 1
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.connect((process[1], int(process[2])))
                except:
                    s = None
                    if(process in processes):
                        processes.remove(process)
            if not processes:
                print "Everything dead"
                exit(0)
        elif len(buffer) > 0:
            print "Value = " + buffer # Prints the value after a get operation was sent
        go = 1

if __name__ == "__main__":
    if(len(sys.argv) != 4):
        print('Usage: python %s <config file name> <server pair id> <client id>' % sys.argv[0]) #usage
        exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3])


'''
LINEARIZABILITY (from the view of a server):
client connects to server replica (done)
server replica creates a thread to listen on from that client and gives that thread an id (1) (done)
user inputs put x 3 (done)
client 1 sends px3 to server replica, id = 2 (done)
client 1 blocks waiting for acknowledgement (done)
server receives px3 and stores 1px3 in a list (done)
server replica sends 2px3 to sequencer (done)
sequencer stores a tuple of (2px3, # of replica servers) in a list and multicasts mpx3 to all server replicas (done)
server replica receives mpx3 and changes x to 3 and sends apx3 to sequencer
sequencer waits till all apx3 are received (second value in tuple is 0) and then removes m2px3 from the list
sequencer sends apx3 to server replica, id = 2
server removes 1px3 from the list and sends a to client 1 
client prints "Acknowledged"
client opens up for next input
'''


'''
EVENTUAL CONSISTENCY (from the view of a server):
client connects to server replica
server replica creates a thread to listen on from that client and gives that thread an id (1)
user inputs put x 3
client 1 sends px3 to server replica, id = 2
client 1 blocks waiting for acknowledgement
server replica receieves px3 and stores a tuple of 1px3 and the value of W in a list
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
client prints "Acknowledged"
client opens up for next input
'''