import socket
import sys
import time
from threading import Thread, Lock

go = 1

def main(config, server_id):
    server = parse_file(config, server_id).split()

    client_thread = Thread(target=create_client, args=(server[1], server[2])) #start client
    client_thread.daemon = True
    client_thread.start()

    while True:
        time.sleep(1) #keep main function running


def parse_file(file_name, server_id):
    i = 0
    with open(file_name) as f:
        for line in f:
            if(i < 2):
                i += 1
            elif(line.split()[0] == server_id):
                return line
'''
Creates the client for the process and reads in input from the command line.
Depending on whether it is multicast or unicast, the code will adapt accordingly.
'''
def create_client(ip, port):

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.connect((ip, int(port)))
    except:
        print("unable to connect: other process may not have been started")
        exit(1)

    read_client = Thread(target=read_client, args=(s))
    read_client.daemon = True
    read_client.start()

    read_server = Thread(target=read_server, args=(s))
    read_server.daemon = True
    read_server.start()

def read_client(s):
    while True:
        message = raw_input('')
        while go == 0: # Wait until the previous request is acknowledged
            time.sleep(1)
        message_split = message.split()
        send_message = -1
        if(message_split[0] == "get"):
            send_message = "g" + message_split[1]

        elif(message_split[0] == "put"):
            send_message = "p" + message_split[1] + message_split[2]

        elif(message_split[0] == "dump"):
            send_message = "d"

        elif(message_split[0] == "delay"):
            time.sleep(float(message_split[1])/1000.0)

        else:
            print "invalid message"

        if(send_message != -1):
            s.send(send_message)
            go = 0

def read_server(s):
    while True:
        buffer = s.recv(128)
        if(buffer == "a"):
            print "Acknowledged" # Acknowledgesw dumps and puts
        else:
            print "Value = " + buffer # Prints the value after a get operation was sent
        go = 1

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python %s <config file name> <server pair id>' % sys.argv[0]) #usage
        exit(1)
    main(sys.argv[1], sys.argv[2])


'''
LINEARIZABILITY (from the view of a server):
client connects to server replica
server replica creates a thread to listen on from that client and gives that thread an id (1)
user inputs put x 3
client 1 sends px3 to server replica, id = 2
client 1 blocks waiting for acknowledgement
server receives px3 and stores 1px3 in a list
server replica sends m2px3 to sequencer
sequencer stores a tuple of (m2px3,# of replica servers) in a list and multicasts mpx3 to all server replicas
server replica receives mpx3 and changes x to 3 and sends apx3 to sequencer
sequencer waits till all apx3 are received (second value in tuple is 0) and then removes m2px3 from the list
sequencer sends apx3 to server replica, id = 2
server removes 1px3 from the list and sends a to client 1 
client prints "Acknowledged"
client opens up for next input
'''
