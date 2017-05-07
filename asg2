#!/usr/bin/env python

import socket
import time
import threading
import sys
import Queue
import copy

class Message(object):
    MARKER_TYPE = 0
    MONEY_TRANSFER_TYPE = 1
    DONE_TYPE = 2

    def __init__(self, source_id, snap_id, amount, type):
	self.source_id = source_id
        self.snap_id = snap_id
        self.amount = amount
        self.type = type
    
    def __str__(self):
	res = str(self.source_id) + " " + str(self.snap_id) + " " + str(self.type) 
    	if self.amount != None:
	   res += " " + str(self.amount)
	res += "||"
    	return res
    	
    def __repr__(self):
	return self.__str__()

    @staticmethod
    def build_string(str):
	keyWords = str.strip().split()
        source_id = int(keyWords[0])
        snap_id = keyWords[1]
        msg_type = int(keyWords[2])
        amount = None
        if msg_type == Message.MONEY_TRANSFER_TYPE:
	    amount = int(keyWords[3])
        if msg_type == Message.DONE_TYPE:
	    amount = int(keyWords[3]) 
        return Message(source_id, snap_id, amount, msg_type)

    @staticmethod
    def split(str):
	res = []
	for msg in str.strip().split("||"):
	    res.append(msg)
	del res[-1]
	return res

class Site(object):
    def __init__(self, site_id):
	self.id = site_id
    	self.snap_count = 0
    	self.snap_count = 0
    	self.balance = 10
    	self.incoming_channels = []
    	self.SnapIDTableLastEntryTemplate = {}
    	self.addr_book = []
    	self.outgoing_channels = {}
    	self.listeningSocket = None
    	self.snapID_table = {}
	self.done_processes = set()

    def open_receive_sock(self, IP, port):
	self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listeningSocket.bind( (IP, port) )
        self.listeningSocket.setblocking(0) 
        self.listeningSocket.listen(1)

    def addOutConnect(self, dest):
	self.outgoing_channels[dest] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def openOutConnections(self):
	for dest, sock in self.outgoing_channels.iteritems():
	    while True:
		try: 
		    sock.connect(self.addr_book[dest - 1])
		    break
		except Exception:
		    continue

    def addInConnect(self, source_id):
	self.SnapIDTableLastEntryTemplate[source_id] = [False, 0]

    def openInConnections(self):
	while len(self.incoming_channels) != len(self.SnapIDTableLastEntryTemplate):
	    try:
	        con, _ = self.listeningSocket.accept()
	        con.setblocking(0)
	        self.incoming_channels.append(con)
	    except socket.error:
		continue
		
    def execute(self, command):
	self.check_message()
        keyWords = command.split()
        if "send" == keyWords[0]:
	    dest = int(keyWords[1])
	    amount = int(keyWords[2])
	    self.transfer_dosh(dest, amount)
	elif "snapshot" == keyWords[0]:
	    self.take_snap()
	elif "sleep" == keyWords[0]:
	    time = float(keyWords[1])
	    self.sleep(time)
	else:
	    print "command not found: " + command
	    exit(1)
	self.check_message()

    def check_message(self):
	BUF_SIZE = 1024
	for con in self.incoming_channels:
	    try:
		msgs = con.recv(BUF_SIZE)
		for msg in Message.split(msgs):
		    msg = Message.build_string(msg.strip())
		    if msg.type == Message.MARKER_TYPE:
			if msg.snap_id not in self.snapID_table: 
			    counter = 1
			    site_state = self.balance 
			    incoming_channels_states = copy.deepcopy(self.SnapIDTableLastEntryTemplate)
			    self.snapID_table[msg.snap_id] = [counter, site_state, incoming_channels_states]
			    self.snapID_table[msg.snap_id][2][msg.source_id][0] = True
			    self.send_mark(msg.snap_id)
			    if self.snapID_table[msg.snap_id][0] == len(self.incoming_channels): 
				self.printSnap(msg.snap_id)
			else: 
			    self.snapID_table[msg.snap_id][0] += 1 
			    self.snapID_table[msg.snap_id][2][msg.source_id][0] = True 
			    if self.snapID_table[msg.snap_id][0] == len(self.incoming_channels): 
				self.printSnap(msg.snap_id)
		    elif msg.type == Message.MONEY_TRANSFER_TYPE: 
			self.balance += msg.amount 
			for _, v in self.snapID_table.iteritems():
			    if v[0] == len(self.incoming_channels): 
				continue
			    if v[2][msg.source_id][0] == False: 
				v[2][msg.source_id][1] += msg.amount 
		    elif msg.type == Message.DONE_TYPE:
			done_process = msg.amount
			if done_process not in self.done_processes:
			    self.done_processes.add(done_process)
			    self.send_done_signal(done_process)
		    else:
			print "ERROR: message type not found"
			print msg
			exit(1)
	    except socket.error, e:
		continue

    def printSnap(self, snap_id):
	output = snap_id + ": "
	output += str(self.snapID_table[snap_id][1]) + " "
        l = self.snapID_table[snap_id][2].items()
	sorted(l, key=lambda item: item[0])
        for _, (_, val) in l:
	    output += str(val) + " "
        output = output.strip()
	print output
        del self.snapID_table[snap_id]

    def transfer_dosh(self, dest, amount):
	self.balance -= amount
        msg = Message(self.id, None, amount, Message.MONEY_TRANSFER_TYPE)
        self.outgoing_channels[dest].send(str(msg))

    def send_mark(self, snap_id):
	msg = Message(self.id, snap_id, None, Message.MARKER_TYPE)
	for dest, sock in self.outgoing_channels.iteritems():
	    sock.send(str(msg))

    def send_done_signal(self, done_process_id):
	msg = Message(self.id, None, done_process_id, Message.DONE_TYPE)
	for dest, sock in self.outgoing_channels.iteritems():
	    sock.send(str(msg))
    
    def take_snap(self):
	self.snap_count += 1
        counter = 0
        snap_id = str(self.id) + "." + str(self.snap_count)
        site_state = self.balance 
        incoming_channels_states = copy.deepcopy(self.SnapIDTableLastEntryTemplate)
        self.snapID_table[snap_id] = [counter, site_state, incoming_channels_states]
        self.send_mark(snap_id)

    def sleep(self, amount):
	count = (amount * 1000)/200
	i = 0
	while(i < count):
	    time.sleep(0.2)
	    self.check_message()
	    i += 1	

    def done_status(self):
	return (self.get_unpolished() == 0) and (len(self.done_processes) == len(self.addr_book))

    def get_unpolished(self):
	return len(self.snapID_table)

    def destruct(self):
	for _, sock in self.outgoing_channels.iteritems():
	    sock.close()
	    for sock in self.incoming_channels:
	        sock.close()
        self.listeningSocket.close()
        exit(0)



#takes in args of format 0 = siteid, 1 = setup, 2 = command
def main():
    if(len(sys.argv) != 4):
        print("correct usage: ./asg2 {site_id} {setup.txt} {command.txt}")
	exit(1)
    site_num = int(sys.argv[1])
    s = Site(site_num)
    setup_f = sys.argv[2]
    command_f = sys.argv[3]
    setup(s, setup_f)
    do_command(s, command_f)


#parse setup file-->sets up sites
def setup(s, setup):
    with open(setup, 'r') as f:
	N = int(f.readline().strip())
        s.num_proc = N
	process = 0
        for line in f.readlines():
	    process += 1
	    if process <= N:
	        IP, port = line.strip().split()
	        port = int(port)
	        s.addr_book.append( (IP, port) )
	        if process == s.id:
		    s.open_receive_sock( IP, port ) 
	    else:
	        source, dest = line.strip().split()
	        source = int(source)
	        dest = int(dest)
	        if source == s.id: #send
		    s.addOutConnect(dest)
		if dest == s.id: #receiver
		    s.addInConnect(source)
    s.openOutConnections()
    s.openInConnections()

#reads command file-->use commands
def do_command(s, c_file):
    with open(c_file, 'r') as f:
	for command in f.readlines():
	    command = command.lower().strip()
	    s.execute(command)
    s.done_processes.add(s.id)
    s.send_done_signal(s.id)
    while True:
	if s.done_status() == True:
	    s.destruct()
	s.check_message()

if __name__ == "__main__":
    main()
