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
min_delay = 0
max_delay = 0
vector_mutex = Lock()
hold_back_queue = Q.PriorityQueue()


def main(argv):
    parsed_file = parse_file(argv[0])
    processes = parsed_file[0]
    min_delay = float(parsed_file[1]/1000)
    max_delay = float(parsed_file[2]/1000)

    found_process = -1

    for process in processes:
        if(argv[1] in process):
            found_process = process

    vector_timestamps.append(0)
    vector_timestamps.append(0)

    try:
        #server and client
        t3 = threading.Thread(target=create_server, args = (min_delay, max_delay, found_process[1], int(found_process[2]), found_process[0], len(processes)))
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
                temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, process_id, client_sockets[i], process_id, id_to_send, False, processes))
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
                temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, process_id, s, process_id, id_to_send, False, processes))
                temp_thread.daemon = True
                temp_thread.start()

'''
Multicast a message from the current process to all the processes in the group
'''
def multicast(message_to_send, processes, found_process, min_delay, max_delay):
    process_id = int(found_process[0])
    for process in processes:
        if((int(process[0]) not in client_socket_ids)):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect((found_process[1], int(process[2])))
            except:
                print("unable to connect: other process may not have been started")
                continue
            client_sockets.append(s)
            client_socket_ids.append(int(process[0]))

    for process in processes:
        if(process_id != int(process[0])):
            print("Sent " + message_to_send + "to process " + str(process[0]) + ", system time is " + str(datetime.datetime.now()))
    
    #Send to Sequencer
    #Increment the internal tracker to keep track of total sent messages
    #If process 2 sends a bunch of messages, it needs to increment the vector_timestamps[1] by 1
    #so that it doesn't miss this number when it gets it's own messages
    #print("changing vector 1 value")
    vector_mutex.acquire()
    vector_timestamps[1] += 1
    vector_mutex.release()
    if(process_id != 0):
        temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, 0, client_sockets[0], process_id, client_socket_ids[0], True, processes))
        temp_thread.daemon = True
        temp_thread.start()
    else:
        #sequence_number = vector_timestamps[1]
        vector_timestamps[0] += 1
        sequence_number = vector_timestamps[0]
        for i in range(len(client_sockets)):
            #if(client_socket_ids[i] != 0):
                temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, sequence_number, client_sockets[i], process_id, client_socket_ids[i], False, processes))
                temp_thread.daemon = True
                temp_thread.start()

        
'''
This method takes in metadata about the process you are and the process you want to send to.
Check if the process that is sending is also the sequencer. If it is the sequencer, then the metadata
is different.
'''
def sendMessage(min_delay, max_delay, message, sequence_number, s, process_id, process_sending_to, sequencer, processes):
    
    message_object = {}
    
    if(sequencer):
        message_object = {
        'sequencer': "sequencer",
        'message': str(message),
        'process_id': str(process_id),
        'processes': processes,
        }

    else:
        message_object = {
        'message': str(message),
        'process_id': str(process_id),
        'sequence_number': sequence_number,
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
def create_server(min_delay, max_delay, host, port, id, num_processes):
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
        read_thread = threading.Thread(target = read_server, args= (min_delay, max_delay, conn, client_connections) )
        read_thread.start()
        
    for conn in client_connections:
        conn.close()

def multicastObject(min_delay, max_delay, message_object, processes, process_id):
    found_process = processes[int(process_id)]
    for process in processes:
        if((int(process[0]) not in client_socket_ids)):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.connect((found_process[1], int(process[2])))
            except:
                print("unable to connect: other process may not have been started")
                continue
            client_sockets.append(s)
            client_socket_ids.append(int(process[0]))

    for i in range(len(client_sockets)):
        #print("Sent " + message_to_send + "to process " + str(client_socket_ids[i]) + ", system time is " + str(datetime.datetime.now()))
        data_loaded = pickle.loads(message_object)
        message_to_send = data_loaded['message']
        sequence_number = data_loaded['sequence_number']
        #if(client_socket_ids[i] != int(process_id)):
        temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, sequence_number, client_sockets[i], process_id, client_socket_ids[i], False, processes))
        temp_thread.daemon = True
        temp_thread.start()

#Each thread is for a different process.
'''
Each thread in here is responsible for communicating with a different process.
Much of the logic behind Total Ordering is located here.
'''
def read_server(min_delay, max_delay, conn, client_connections):
    while True:
        data = conn.recv(1024)
        if(not data or str(data) == 'exit'): 
            break

        #Check if a sequencer is multicasting
        data_loaded = pickle.loads(data)
        #print(data_loaded['message'])
        if('sequencer' in data_loaded):
            processes = data_loaded['processes']
            process_id = data_loaded['process_id']
            message_received = data_loaded['message']

            #global timestamp index
            #only process 0 can increment this
            vector_timestamps[0] += 1
            sequence_number = vector_timestamps[0]
            #print("sequence_number: " + str(sequence_number) + " message: " + str(message_received))
            message_object = {
            'message': message_received,
            'sequence_number': sequence_number,
            'process_id': process_id
            }

            data_serialized = pickle.dumps(message_object, -1)
            temp_thread = threading.Thread(target = multicastObject, args = (min_delay, max_delay, data_serialized, processes, process_id))
            temp_thread.daemon = True
            temp_thread.start()

        #Check if any other process is listening in for input
        else:
            id_received = data_loaded['process_id']
            sequence_number = data_loaded['sequence_number']
            message_received = data_loaded['message']
            print(message_received)
            #print("sequence_number: " + str(sequence_number) + " id_received: " + str(id_received) + " vector[1]: " + str(vector_timestamps[1]) + " message_received: " + str(message_received))
            if(sequence_number == (vector_timestamps[1] + 1) or sequence_number == vector_timestamps[1]):
                print("Received " + str(message_received) + "from process " + str(id_received) + ", system time is " + str(datetime.datetime.now()))
                #print("changing vector 1 value")
                vector_mutex.acquire()
                vector_timestamps[1] += 1
                vector_mutex.release()
                if(hold_back_queue.empty() == False):
                    temp_thread = threading.Thread(target=checkHoldBackQueue, args = (hold_back_queue, vector_timestamps, id_received))
                    temp_thread.daemon = True
                    temp_thread.start()
            elif(sequence_number > (vector_timestamps[1] + 1)):
                #print("Placing into hold back queue")
                #print("sequence_number: " + str(sequence_number) + " id_received: " + str(id_received) + " vector[1]: " + str(vector_timestamps[1]) + " message_received: " + str(message_received))
                hold_back_queue.put((sequence_number, id_received, message_received))


'''
Check the hold back queue for items whenever a message from the buffer should be removed
and delivered ("printed") to the screen.
'''
def checkHoldBackQueue(hold_back_queue, vector_timestamps, id_received):
    while True:
        if(hold_back_queue.empty()):
            return
        value = hold_back_queue.get(block=True)
        #print("Removed from hold_back_queue")
        #print(value)
        if(value[0] == (vector_timestamps[1] + 1)):
            print("Received " + str(value[2]) + "from process " + str(id_received) + ", system time is " + str(datetime.datetime.now()))
        else:
            hold_back_queue.put(value)
            return
        #print("changing vector 1 value")
        vector_mutex.acquire()
        vector_timestamps[1] = value[0]
        vector_mutex.release()
        #print("Removing from the queue")
        hold_back_queue.task_done()

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python %s <config file name> <process number>' % sys.argv[0]) #usage
        exit(1)
    main(sys.argv[1:])

