import socket
import sys
import time
from threading import Thread, Lock

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

    while True:
        message = raw_input('')
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

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python %s <config file name> <server pair id>' % sys.argv[0]) #usage
        exit(1)
    main(sys.argv[1], sys.argv[2])
