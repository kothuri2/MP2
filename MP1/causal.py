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

'''
Parse the config file for the data about the processes and the min_delay, max_delay
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
            
            #vector_mutex.acquire()
            vector_timestamps[process_id] += 1
            #vector_mutex.release()
            sequence_number = vector_timestamps[process_id]
            for i in range(len(client_sockets)):
                #message = message_to_send + " " + str(found_process[0])
                #simulate random network delay
                #print("Sent " + message_to_send + "to process " + str(client_socket_ids[i]) + ", system time is " + str(datetime.datetime.now()))
                temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, sequence_number, client_sockets[i], vector_timestamps, process_id, client_socket_ids[i]))
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
                    #message = message_to_send + " " + str(found_process[0])
                    #simulate random variable network delay
                    print("Sent " + message_to_send + "to process " + str(id_to_send) + ", system time is " + str(datetime.datetime.now()))
                    temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message_to_send, process_id, client_sockets[i], vector_timestamps, process_id, client_socket_ids[i]))
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
                    #message = message_to_send +  " " + str(found_process[0])
                    #simulate random network delay
                    print("Sent " + message_to_send + "to process " + str(id_to_send) + ", system time is " + str(datetime.datetime.now()))
                    temp_thread = threading.Thread(target=sendMessage, args = (min_delay, max_delay, message, process_id, s, vector_timestamps, process_id, client_socket_ids[i]))
                    temp_thread.daemon = True
                    temp_thread.start()
                    #time.sleep(random.randrange(min_delay, max_delay))
                    #s.sendall(str(message))

        if(message == 'exit'):
            break

    for s in client_sockets:
        s.close()

'''
This method takes in metadata about the process you are and the process you want to send to.
'''
def sendMessage(min_delay, max_delay, message, sequence_number, s, vector_timestamps, process_id, process_sending_to):
    
    message_object = {
    'message': str(message),
    'process_id': str(process_id),
    'vector_timestamps': vector_timestamps
    }
    #print("sending Message")
    #print("process_id: " + str(process_id))
    #print("process_sending_to: " + str(process_sending_to))
    data_serialized = pickle.dumps(message_object, -1)

    #Hardcoded Causal Ordering Testcase
    '''
    if(int(process_id) == 0 and int(process_sending_to) == 2):
        #print("Sending to process: " + str(process_sending_to)+ " from " + str(process_id) + " delay for: 10")
        time.sleep(10)
    if(int(process_id) == 0 and int(process_sending_to) == 1):
        #print("Sending to process: " + str(process_sending_to)+ " from " + str(process_id) + " delay for: 1/1000")
        time.sleep(1/1000)
    if(int(process_id) == 1 and int(process_sending_to) == 2):
        #print("Sending to process: " + str(process_sending_to)+ " from " + str(process_id) + " delay for: 2")
        time.sleep(1/1000)
    if(int(process_id) == 1 and int(process_sending_to) == 0):
        #print("Sending to process: " + str(process_sending_to)+ " from " + str(process_id) + " delay for: 3")
        time.sleep(3)
    '''
    time.sleep(random.randrange(min_delay, max_delay))
    s.sendall(data_serialized)
    return

'''
This method sets up the server using SBLA protocol and listens and accepts for other processes
to connect to it. In addition, it spins up a new thread each time a new connection is made so that that
sole thread is responsible for taking in input from that process.
'''
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
'''
Each thread in here is responsible for communicating with a different process.
Much of the logic behind Causal Ordering is located here.
'''
def read_server(vector_timestamps_server, conn):
    while True:
        data = conn.recv(1024)
        if(not data or str(data) == 'exit'): 
            break

        data_loaded = pickle.loads(data)
        vector_timestamps_client = data_loaded['vector_timestamps']
        message_received = str(data_loaded['message'])
        process_id = int(data_loaded['process_id'])
        #print("receiving message")
        #print(message_received)
        #print(vector_timestamps_client)
        '''
        Send vector timestamp over from client to server 
        receive vector timestamp and compare with server

        if the client has 1 greater, accept
        if the client has at least 1 great and at least 1 less than, accept
        if the client has more than 1 greater, queue

        if accept, check if first element on queue has 1 greater
        if yes, accept (remove from queue)
        if no, move on (dont touch queue)
        '''
        
        #vector_mutex.acquire()
        countGreater = 0
        countLessThan = 0
        countExact = 0
        
        for i in range(len(vector_timestamps_server)):
            #Counts the number of elements in the vector that differ by exactly one
            #If this count is only 1, and none that are less than each other, then we have the basic
            #case of causal ordering
            if(i == process_id):
                if(vector_timestamps_client[i] == (vector_timestamps_server[i] + 1)):
                    countExact += 1

            #Counts the number of elements that are bigger in the server than the 
            #client
            else:
                if(vector_timestamps_server[i] > vector_timestamps_client[i]):
                    countLessThan += 1
            
                #Counts the number of elements that are bigger in the client than the 
                #server
                elif(vector_timestamps_server[i] < vector_timestamps_client[i]):
                    countGreater += 1

        #Case whenever there is only one greater timestamp between
        #the sending process and the receiving process
        #Ex: sender timestamp_1 -> (1,0,0)
        #receiver timestamp_2 -> (0,0,0)
        #The receiver modifies its own timestamps and copies it over
        if(countExact == 1 and countGreater == 0):
            print("Received " + str(message_received) + "from process " + str(process_id) + ", system time is " + str(datetime.datetime.now()))
            for i in range(len(vector_timestamps_server)):
                vector_timestamps_server[i] = max(vector_timestamps_client[i], vector_timestamps_server[i])
            temp_thread = threading.Thread(target=checkHoldBackQueue, args = (hold_back_queue, vector_timestamps_server) )
            temp_thread.daemon = True
            temp_thread.start()

        #Concurrent events case
        #Neither one vector was entirely less than the either       
        elif(countExact == 0 and countGreater >= 1 and countLessThan >= 1):
            print("Received " + str(message_received) + "from process " + str(process_id) + ", system time is " + str(datetime.datetime.now()))
            for i in range(len(vector_timestamps_server)):
                vector_timestamps_server[i] = max(vector_timestamps_server[i], vector_timestamps_client[i])
            temp_thread = threading.Thread(target=checkHoldBackQueue, args = (hold_back_queue, vector_timestamps_server) )
            temp_thread.daemon = True
            temp_thread.start()

        #Buffer the message here
        else:
            #print("placing into queue")
            hold_back_queue.put((vector_timestamps_client, process_id, message_received))
        

        #vector_mutex.release()

def checkHoldBackQueue(hold_back_queue, vector_timestamps_server):
    while True:
        #print(hold_back_queue.qsize())
        if(hold_back_queue.empty()):
            return
        value = hold_back_queue.get(block=True)
        vector_timestamps_client = value[0]
        process_id = value[1]
        message_received = value[2]

        countGreater = 0
        countLessThan = 0
        countExact = 0

        for i in range(len(vector_timestamps_server)):
            #Counts the number of elements in the vector that differ by exactly one
            #If this count is only 1, and none that are less than each other, then we have the basic
            #case of causal ordering
            if(i == process_id):
                if(vector_timestamps_client[i] == (vector_timestamps_server[i] + 1)):
                    countExact += 1
                    
            #Counts the number of elements that are bigger in the server than the 
            #client
            else:
                if(vector_timestamps_server[i] > vector_timestamps_client[i]):
                    countLessThan += 1
            
                #Counts the number of elements that are bigger in the client than the 
                #server
                elif(vector_timestamps_server[i] < vector_timestamps_client[i]):
                    countGreater += 1

        if(countExact == 1 and countGreater == 0):
            print("Received " + str(message_received) + "from process " + str(process_id) + ", system time is " + str(datetime.datetime.now()))
            for i in range(len(vector_timestamps_server)):
                vector_timestamps_server[i] = max(vector_timestamps_client[i], vector_timestamps_server[i])
        elif(countExact == 0 and countGreater >= 1 and countLessThan >= 1):
            print("Received " + str(message_received) + "from process " + str(process_id) + ", system time is " + str(datetime.datetime.now()))
            for i in range(len(vector_timestamps_server)):
                vector_timestamps_server[i] = max(vector_timestamps_client[i], vector_timestamps_server[i])
        else:
            hold_back_queue.put(value)
            #vector_mutex.release()

        #vector_mutex.release()

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python %s <config file name> <process number>' % sys.argv[0]) #usage
        exit(1)
    main(sys.argv[1:])

