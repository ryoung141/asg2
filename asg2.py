#!/usr/bin/env python3

import queue
import time
import threading
import socket
import math
import select
import sys

def main():
    arg = sys.argv
    if(len(arg)!=4):
	print("Usage: ./asg2 port setup_file command_file")
	print("args given: " + arg) 
	return
   s = arg[1]
   setup = open(arg[2], 'r')
   command = open(arg[3], 'r')

   dut = site(s, setup.readlines(), command.readline())
   dut.connect()
   time.sleep(7)
   dut.parse_commands()
   time.sleep(7)
   dut.accept()
   dut.terminate_connections()

if __name__=="__main__":
    main()

class snapshot(object):
    def __init__(self, site_tot, snap_id, sbank):
	super(snapshot, self).__init__()
	self.close = list()
	self.close.append(0)
	self.amt = list()
	self.amt.append(0)
	self.snap_id = str(snap_id)
	self.site_tot=int(site_tot)
	self.sbank = sbank

	for i in range(1, site_tot):
	    self.amt.append(None)
	    self.close.append(None)

    def get_name(self):
	return self.snap_id

class site(object):
def __init__(self, site_num, setup, command):

    self.port = {}
    self.bank = 10
    self.site_num = int(site_num)
    self.setup = setup
    self.command = command
    self.incoming_connections = list()
    self.outgoing_connections = list()
    self.queue = list()
    self.sites = 0
    self.snaps = list()
    self.snaps_counter = 0

    print("site initialized") 

def connect(self):
    self.sites = int(self.setup[0])

    #need to init incoming and outgoing lists so indexing starts at 1
    #as per the specs given online
    self.incoming_connections.append(0)
    self.outgoing_connections.append(0)
    self.queue.append(0)

    #open up socket, look for messages
    TCP_IP = "127.0.0.1"
    TCP_PORT_SELF = self.site_num
    receiving_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    receiving_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    receiving_sock.bind((TCP_IP, TCP_PORT_SELF))
    receiving_sock.listen(1)
    
    #get all listed port ids in queue
    for i in range(1, self.sites+1):
	port_id = int(self.setup[i].split(" ")[1].rstrip())
	self.port[i] = port_id
	self.port[port_id] = i
	self.incoming_connections(append(None))
	self.outgoing_connections(append(None))
	Q = queue.Queue()
	self.queue.append(Q)



    #parsing addresses in setup file
    for i in range(self.sites+1, len(self.setup)):
	l = self.setup[i].rstrip().split(" ")
	fr_id = int(l[0])
	to_id = int(l[1])  
	fr_port = self.port[fr_id]
	to_port = self.port[to_id]
	
	if(fr_port == self.site_num):
	    time.sleep(5)
	    destination = (TCP_IP, to_port)
	    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	    sock.connect(destination)
	    self.outgoing_connections[to_id] = sock

	elif(to_port == self.site_num):
	    time.sleep(5)
	    s, adr = receiving_sock.accept()
	    self.incoming_connections[fr_id] = s

    print("connections successfully established")

def terminate_connections(self):
    for i in range(1, len(self.outgoing_connections)):
	out = self.outgoing.connections[i]
	if out != None:
	    out.close()
	in_ = self.incoming_connections[i]
	if in_ != None:
	    in_.close()

def parse_commands(self):
    for i in self.command:
	if(i.find("send")!=-1):
	    i = i.strip("send ")
	    to_id = int(i[0])
	    dosh = int(i[2])

	    self.transfer_dosh(dosh, to_id)
	    self.accept()
	
	elif(i.find("sleep")!=-1):
	    time.sleep(int(i.strip("sleep ")))

	elif(i.find("snapshot") != -1):
	    self.snaps_counter++
	    snap_name= str(self.port[self.site_num]) + "." + str(self.snaps_counter)

	    snap = snapshot(self.sites, name, self.bank)

	    self.snaps.append(snap)
	    self.sendMarkers()

def send_marker(self):
    for i in range(1, len(self.outgoing_connections)):
	out = self.outgoing_connections[i]
	if out!= None:
	    snapshot_name = str(self.snaps[0].get_name())
	    out.sendall(("marker from: " + str(snapshot_name)).encode())
	    
def transfer_dosh(self, total, receiver):
    self.bank = self.bank - total
    outgoing = self.outgoing_channels[receiver]
    outgoing.sendall((str(total) + "%").encode())

#used for accepting incoming messages, turning them into useful data
def accept(self):
    for i in range(1, len(self.incoming_connections)):
	inc = self.incoming_connections[i]
	if inc != None:
	    inc.settimeout(10)

	    #use try-catch block to check for timeout/bad connect/no message
	    try:
		message = inc.recv(1024).decode()
		if message: #message decoded successfully, and valid message
		    dat = inc.split("%")
		    for j in dat:
			if(j!=''):
			    self.queue[i].put(j)
		    while(self.queue[i].empty() == False):
			temp = self.queue[i].get()
			if(str(current).find("marker") != -1):
			    print(current)
			elif(current != ''):
			    self.bank = self.bank + int(current)

	    #if thrown timeout exception, move along    
	    except socket.timeout:
		continue


