import collections
import sys
import socket
import time
import ast
import Queue
from threading import Thread

'''
KNOWN PROBLEMS:

    1. Need to create initial finger tables by circling around the chord instead of initializing from the client_thread (Double Damnit Kush -_-)
    2. Finger Tables not updating correctly (probably an issue with the mods as it seems to work without wrap around)
    3. Keys aren't updating correctly (predecessor ids are most likely not being passed in correctly)
        --> Determine predecessor ids at initialization rather than when updating keys

'''

def main(config):
    parts = parse_file(config)

    min_delay = parts[0][0]
    max_delay = parts[0][1]
    initial_port = parts[1]
    client_thread = Thread(target=client, args=(initial_port)) #start client
    client_thread.daemon = True
    client_thread.start()

    while True:
        time.sleep(1)

def parse_file(config):
    parts = []
    with open(config) as f:
        for line in f:
            if(len(line.split()) != 0):
                parts.append(line.split())
    return parts

def client(initial_port):
    global go
    go = 1
    nodes = {} # hash node_id to the socket

    print_thread = Thread(target = start_printing, args=([8000 - 1])) #start printing from the client
    print_thread.daemon = True
    print_thread.start()

    port = int(initial_port) #Start Node 0 on  initial port
    node_thread = Thread(target=start_node, args=([port])) #start each node
    node_thread.daemon = True
    node_thread.start()
    nodes['0'] = get_socket(port)
    while (not nodes['0']):
        nodes['0'] = get_socket(port) # Continue trying to connect till it connects
    nodes['0'].sendall(get_fingers('0', nodes))

    while True:
        message = raw_input('')

        while not go:
            time.sleep(1)
        go = 0
        command = message.split()[0]
        try:
            node_id = message.split()[1] #i.e. Node "18"
        except:
            print "Invalid message"
            continue
            node_id = -1

        if command == "join": #join p

            if(node_id in nodes):
                print "Node already exists"
                continue

            port = int(node_id) + int(initial_port) #i.e. Node 18 is on port 8018

            node_thread = Thread(target=start_node, args=([port])) #start each node
            node_thread.daemon = True
            node_thread.start()

            nodes[node_id] = get_socket(port)
            while (not nodes[node_id]):
                nodes[node_id] = get_socket(port) # Continue trying to connect till it connects
            nodes = collections.OrderedDict(sorted(nodes.items())) #Sort the nodes in increasing order
            nodes[node_id].sendall(get_fingers(node_id, nodes))
            
        elif command == "find": #find p k

            nodes[node_id].sendall(message) #send the key that is being searched for

        elif command == "crash": #crash p

            nodes[node_id].sendall("crash") #tell the node to crash
            del nodes[node_id] #remove the node from the list of connections

        elif command == "show": #show p

            if(node_id == "all"):
                for socket in nodes:
                    nodes[socket].sendall("show") #send the key that is being searched for
            else:
                if(node_id in nodes):
                    nodes[node_id].sendall("show") #send the key that is being searched for
                else:
                    print "N" + node_id + " isn't a valid Node"

        else:
            print "Invalid message"
            go = 1

def start_printing(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(33) #up to 31 connections from other nodes, 1 from client, and 1 extra to be safe

    while True:
        conn, addr = s.accept()
        print_thread = Thread(target = print_from_conns, args= ([conn]))
        print_thread.daemon = True
        print_thread.start()

def print_from_conns(conn):
    global go
    while True:
        data = conn.recv(1024)

        if(data == ""):
            continue

        else:
            print(data)
            go = 1

def get_fingers(node_id, nodes): #Create node's finger table
    finger_table = {0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0}
    for node in nodes:
        for i in range(8):
            val = (int(node_id) + 2**i)%255
            if int(node) > val and (val < finger_table[i] or finger_table[i] == 0):
                finger_table[i] = int(node)
    return str(finger_table)

def get_socket(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        #print "Connecting to Port", port
        s.connect(("127.0.0.1", int(port)))

        return s
    except:
        return False

def start_node(port):
    keys = [] #list of all keys in this node
    predecessor_keys = [] #list of all keys in the predecessor's node
    finger_table = [] #maps node_ids to sockets
    predecessor_id = 0

    if(port == 8000):
        keys = [x for x in range(256)]

    client = get_socket(8000 - 1) # get socket to client
    while not client:
        client = get_socket(8000 - 1)


    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(33) #up to 31 connections from other nodes, 1 from client, and 1 extra to be safe
    q = Queue.Queue()

    accept_thread = Thread(target = start_accepting, args= (s, q))
    accept_thread.daemon = True
    #print "started accept thread for ", port
    accept_thread.start()

    node_id = port - 8000

    while True:

        data = q.get()

        if(data == "crash"):
            #CRASH THE NODE
            client.sendall("P" + str(node_id) + " Crashed")
            #print "hi"

        elif(data == "show"):
            line = "N" + str(node_id) + "\n"
            temp = []
            for i in range(8):
                temp.append(finger_table[i][0])
            line +=  "FingerTable: " + str(temp) + "\n"
            line +=  "Keys: " + str(keys)
            client.sendall(line)

        elif(data.split()[0] == "find"): # find p k
            initial_port = int(data.split()[1])
            key = int(data.split()[2])
            if(key in keys):
                if(initial_port == node_id):
                    client.sendall(str(node_id)) #communication from client only from p
                else:
                    found = find(initial_port, finger_table, node_id) #Returns tuple of port,socket
                    found[1].sendall("found " + str(initial_port) + " " + str(node_id))
            else:
                found = find(key, finger_table, node_id) #returns tuple of port,socket
                found[1].sendall(data)

        elif(data.split()[0] == "found"): # found initial_port location_port
            initial_port = int(data.split()[1])
            location_port = data.split()[2]
            if(initial_port == node_id):
                client.sendall(location_port) #clinet communication only from initial port
            else:
                found = find(initial_port, finger_table, node_id) #returns tuple of port,socket
                found[1].sendall(data) #if we aren't at the start_port yet, keep looking


        elif(data.split()[0] == "finger"):
            #print "node: " + str(node_id) + " is being updated with " + data.split()[1]
            finger_table = update_fingers(node_id, int(data.split()[1]), finger_table)
            if(len(data.split()) == 3 and data.split()[2] == "keys"):
                #print "enter"
                predecessor_id = int(data.split()[1])
                if(predecessor_id > node_id):
                    keys = [x for x in range(predecessor_id + 1, 256)]
                    keys += (x for x in range(0, node_id + 1))
                else:
                    keys = [x for x in range(predecessor_id + 1, node_id + 1)]   

        elif(data.split()[0] == "predecessor"):
            predecessor_id = int(data.split()[1])
 
        else:
            finger_table = ast.literal_eval(data)
            successor = 1
            prev = -1
            for i in range(8):

                if(finger_table[i] == node_id):
                    finger_table[i] = [node_id, "self"]

                else:
                    if finger_table[i] != prev:
                        s = get_socket(int(finger_table[i]) + 8000)
                    if not s:
                        print "Could not connect to " + str(finger_table[i])
                    
                    finger_table[i] = [finger_table[i], s]
                    
                    if(successor): # Send the node_id to the successor to update the finger_table and the keys flag to update the keys
                        finger_table[i][1].sendall("finger " + str(node_id) + " keys") 
                        successor = 0

                prev = finger_table[i][0]

            client.sendall("P" + str(node_id) + " Joined")

def find(value, finger_table, node_id):
    successor = 1
    prev_node = -1
    sent = 0
    #for node in finger_table:
    for i in range(8):
        if(finger_table[i][0] == node_id):
            successor = 0
            continue
        elif successor and finger_table[i][0] >= value and node_id < value: #the successor is the node you are looking for because the value is between this node and the successor
            return finger_table[i] #return successor's id and socket if it is the chosen one
        else:
            successor = 0
        if finger_table[i][0] > value:
            sent = 1
            return finger_table[i-1] #return the largest node that is smaller than the value
    if not sent:   
        return finger_table[i] #if none of them are greater, then just jump to the last node in the table because thats the closest one


def start_accepting(s, q):
    while True:
        conn, addr = s.accept()
        read_thread = Thread(target = read_from_conns, args= (conn, q))
        read_thread.daemon = True
        #print "started read thread for "
        read_thread.start()

def read_from_conns(conn, q):
    while True:
        data = conn.recv(1024)

        #print "Data:",data

        if(data == ""):
            continue

        else:
            q.put(data)

def update_fingers(node_id, new_finger, finger_table):
    #print finger_table
    if new_finger > (node_id + 2**7):
        return finger_table
    s = False
    successor = 1
    for i in range(8):
        if (new_finger < finger_table[i][0] or finger_table[i][0] == 0) and new_finger >= (node_id + 2 ** i):
            finger_table[i][0] = new_finger

            if not s:
                s = get_socket(new_finger + 8000)
                if not s:
                    print "Could not connect to " + str(new_finger)

            finger_table[i][1] = s

        if(finger_table[i][0] != node_id and successor):
            if(finger_table[i][0] != new_finger):
                finger_table[i][1].sendall("finger " + str(new_finger)) # Send the finger to the successor (assuming its not the original node)
            elif finger_table[len(finger_table)-1][0] == new_finger:
                finger_table[len(finger_table)-1][1].sendall("predecessor " + str(node_id))
            successor = 0

    return finger_table



if __name__ == "__main__":
    if(len(sys.argv) != 2):
        print('Usage: python %s <config file name>' % sys.argv[0]) #usage
        print('join p = create node with node_id p')
        print('show p = show fingertable, keys, and id for node_id p')
        print('show all = show fingertable, keys, and id for all nodes')
        print('find p k = find key k from node p')
        print('crash p = crashes node with node_id p')
        exit(1)
    main(sys.argv[1])
