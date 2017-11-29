#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/12/17 9:56 PM
# @Author  : Ethan
# @Site    :
# @File    : server.py
# @Software: PyCharm

import socket
import time
import threading
import signal
import sys
import copy
import os
import json
import pytz
from datetime import datetime
from dateutil import parser
import config as conf

# serverSocket = socket.socket()         # Create a socket object
# host = socket.gethostname()            # Get local machine name
# port = 12345                           # Reserve a port for your service.
# serverSocket.bind((host, port))        # Bind to the port

host = ''
threadLock = threading.Lock()
fileLock = threading.Lock()
threads = []

MY_ID = 0
MY_PORT = 0
MY_IP = ''
MY_TZ = ''
timezone = pytz.timezone('US/Eastern')
my_clock = 0
id_addr = {}

site_id = 0
clock = 0
states = []                # [[]]
log = []                   # [eR]
partial_log = {}           # {MY_ID + my_clock, Event}
block_dict = {}            # {int, set(int)}

block_to = set()
blocked_by = set()

is_exit = False

class Event(object):
    def __init__(self, my_id, my_clock, operation, time, content):
        self.my_id = my_id
        self.my_clock = my_clock
        self.operation = operation
        self.time = time
        self.content = content


def event2dict(event):
    return {
        'id':        event.my_id,
        'clock':     event.my_clock,
        'operation': event.operation,
        'time':      event.time.strftime('%Y-%m-%d %H:%M:%S %z'),
        'content':   event.content
    }

# hasRecord function is check if target_id site already has event record(eR)
def has_record(eR, target_id):
    return states[target_id][eR.my_id] >= eR.my_clock


class RemoteThread (threading.Thread):
    def __init__(self, thread_id, name, description):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.description = description

    def run(self):
        global states, log, partial_log, block_dict, blocked_by
        print "Server  ", MY_IP, " is listening on port "

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((MY_IP, MY_PORT))

        server_socket.listen(5)  # Now wait for client connection.
        while True:
            # Establish connection with client.
            client_socket, addr = server_socket.accept()
            print 'Got connection from', addr
            # client_socket.send('Thank you for connecting')

            # Get lock to synchronize threads
            threadLock.acquire()

            jresp = client_socket.recv(1024)
            log_message = json.loads(jresp)
            # print "Recv: ", log_message

            if log_message["type"] == "LOG":
                client_id = log_message["id"]
                new_states = log_message["states"]
                recv_msgs = log_message["message"]
                # logs needed to be dealt with
                ne = {}
                with open(conf.log_file_path, 'a') as f:
                    for msg in recv_msgs:
                        timestamp = parser.parse(msg["time"])
                        timestamp = timestamp.astimezone(timezone)
                        eR = Event(int(msg["id"]), int(msg["clock"]), msg["operation"], timestamp, msg["content"])
                        if states[MY_ID][eR.my_id] < eR.my_clock:
                            print event2dict(eR)
                            if msg["operation"] == "block":
                                ne[eR.my_clock * 100 + eR.my_id] = eR
                            elif msg["operation"] == "unblock":
                                ne[eR.my_clock * 100 + eR.my_id] = eR
                            log.append(eR)
                            f.write(json.dumps(event2dict(eR)) + "\n")

                # update the states matrix
                for i in range(len(states)):
                    if states[MY_ID][i] < new_states[client_id][i]:
                        states[MY_ID][i] = new_states[client_id][i]
                for i in range(len(states)):
                    for j in range(len(states)):
                        if states[i][j] < new_states[i][j]:
                            states[i][j] = new_states[i][j]
                # write states to local file
                with open(conf.state_file_path, 'w') as f:
                    line = {}
                    line["states"] = states
                    f.write(json.dumps(line))

                # update the partial log
                for key in partial_log.keys():
                    not_known = False
                    eR = partial_log[key]
                    for k in range(len(id_addr)):
                        if states[k][eR.my_id] < eR.my_clock:
                            not_known = True
                            break
                    if not not_known:
                        partial_log.pop(key)
                for key in ne.keys():
                    eR = ne[key]
                    if eR.operation == "block":
                        if(int(eR.content) == MY_ID):
                            blocked_by.add(eR.my_id)
                        block_dict[eR.my_id].add(int(eR.content))
                    elif eR.operation == "unblock":
                        if (int(eR.content) == MY_ID):
                            blocked_by.remove(eR.my_id)
                        block_dict[eR.my_id].remove(int(eR.content))
                    not_known = False
                    for k in range(len(id_addr)):
                        if states[k][eR.my_id] < eR.my_clock:
                            not_known = True
                            break
                    if not_known:
                        partial_log[key] = eR
                # write partial log to local file
                with open(conf.partial_log_file_path, 'w') as f:
                    for k,eR in partial_log.iteritems():
                        f.write(json.dumps(event2dict(eR)) + "\n")
                # write blocking infor to local file
                with open(conf.blocking_info_file_path, 'w') as f:
                    for key in block_dict.keys():
                        f.write(str(key) + ' ')
                        for val in block_dict[key]:
                            f.write(str(val) + ' ')
                        f.write("\n")

            # Free lock to release next thread
            threadLock.release()

            client_socket.close()  # Close the connection


class LocalThread (threading.Thread):
    def __init__(self, thread_id, name, description):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.description = description

    def run(self):
        while True:
            # parse the command get from Terminal
            command = raw_input("Input command: use 'help' command to view demo commands\n").strip()
            index = command.find(" ")
            if index == -1:
                operation = command
                content = ""
            else:
                operation = command[:command.find(" ")]
                content = command[command.find(" "):].strip()

            # execute specific required command
            if operation == "tweet":
                self.exeTweet(content)
            elif operation == "view":
                self.exeView()
            elif operation == "block":
                self.exeBlock(int(content))
            elif operation == "unblock":
                self.exeUnblock(int(content))
            elif operation == "print":
                self.exePrint(content)
            elif operation == "help":
                print "tweet Hello, world!\nview\nblock 1\nunblock 2\nprint log/block_dict/states/partial_log"

    # exeTweet function is to post a tweet by this site and send to other sites
    def exeTweet(self, content):
        global my_clock, log, block_dict, states

        threadLock.acquire()

        # create an event for tweet operation
        my_clock += 1
        states[MY_ID][MY_ID] = my_clock

        eR = Event(MY_ID, my_clock, "tweet", datetime.now(timezone), content)
        log.append(eR)
        # write to local log file
        with open(conf.log_file_path, 'a') as f:
            f.write(json.dumps(event2dict(eR))+"\n")
        # write states to local file
        with open(conf.state_file_path, 'w') as f:
            line = {}
            line["states"] = states
            f.write(json.dumps(line))

        # send different NP messages to other sites
        for target_id in id_addr.keys():
            # don't need to send message to itself
            if target_id == MY_ID:
                continue

            if target_id in block_to:
                continue

            # find all events that target_id site might not know
            NP = []
            for eR in log:
                if states[target_id][eR.my_id] < eR.my_clock:
                    # serialize the MY_ID, target_id, NP, states variavles
                    NP.append(event2dict(eR))

            # send the message to other sites
            msg = {}
            msg['type'] = "LOG"
            msg["id"] = MY_ID
            msg['message'] = NP
            msg['states'] = states
            print 'trying to connect ', id_addr[target_id]
            peer_message = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection = id_addr[target_id].split(':')
            print json.dumps(msg)

            peer_message.settimeout(5)
            try:
                peer_message.connect((connection[0], int(connection[1])))
                peer_message.send(json.dumps(msg))
            except Exception:
                pass

        threadLock.release()

    # exeView function is to view all tweets from unblocked sites
    # view is a local operation
    def exeView(self):
        global MY_ID, my_clock, log, blocked_by, log

        threadLock.acquire()

        # get all tweet events that are not from blocked sites
        tweets = []
        for eR in log:
            if eR.operation == "tweet" and eR.my_id not in blocked_by:
                tweets.append(eR)

        # sort the tweets
        tweets.sort(key=lambda event: event.time, reverse=True)

        # print the tweets following a certain format
        for eR in tweets:
            print "Site:", eR.my_id, "posted at", str(eR.time), ":", eR.content

        threadLock.release()

    # exeBlock function is to handle block commands
    # block is a local operation
    def exeBlock(self, target_id):
        global my_clock, states, log, partial_log, block_dict, block_to

        # check the validation of target_id
        if target_id == MY_ID:
            print "[Invalid Id] You can not block yourself"
            return

        if target_id not in id_addr.keys():
            print "[Invalid Id] There is no site with id:", target_id
            return

        if target_id in block_dict[MY_ID]:
            print "[Invalid Id] This site is already blocked"
            return

        threadLock.acquire()

        # create an event for block operation & add it to log
        my_clock += 1
        states[MY_ID][MY_ID] = my_clock
        eR = Event(MY_ID, my_clock, "block", datetime.now(timezone), target_id)
        log.append(eR)
        with open(conf.log_file_path, 'a') as f:
            f.write(json.dumps(event2dict(eR))+"\n")
        with open(conf.partial_log_file_path, 'a') as f:
            f.write(json.dumps(event2dict(eR))+"\n")
        # write states to local file
        with open(conf.state_file_path, 'w') as f:
            line = {}
            line["states"] = states
            f.write(json.dumps(line))

        # update block-related variables
        partial_log[my_clock * 100 + MY_ID] = eR
        block_dict[MY_ID].add(target_id)
        block_to.add(target_id)

        # write blocking infor to local file
        with open(conf.blocking_info_file_path, 'w') as f:
            for key in block_dict.keys():
                f.write(str(key) + ' ')
                for val in block_dict[key]:
                    f.write(str(val) + ' ')
                f.write("\n")

        threadLock.release()

    # exeUnblock function is to handle unblock commands
    # unblock is a local operation
    def exeUnblock(self, target_id):
        global MY_ID, my_clock, states, log, partial_log, block_dict, block_to

        # check the validation of target_id
        if target_id not in block_dict.keys():
            print "[Invalid Id] There is no site with id:", target_id
            return

        if target_id not in block_to:
            print "[Invalid Id] This site hasn't been blocked before!"
            print "Current blocked sites are:", [block_id for block_id in block_to]
            return

        threadLock.acquire()

        # create an event for block operation & add it to log
        my_clock += 1
        states[MY_ID][MY_ID] = my_clock
        eR = Event(MY_ID, my_clock, "unblock", datetime.now(timezone), target_id)
        log.append(eR)
        with open(conf.log_file_path, 'a') as f:
            f.write(json.dumps(event2dict(eR))+"\n")
        with open(conf.partial_log_file_path, 'a') as f:
            f.write(json.dumps(event2dict(eR))+"\n")
        # write states to local file
        with open(conf.state_file_path, 'w') as f:
            line = {}
            line["states"] = states
            f.write(json.dumps(line))

        # update block-related variables
        partial_log[my_clock * 100 + MY_ID] = eR
        block_dict[MY_ID].remove(target_id)
        block_to.remove(target_id)

        # write blocking infor to local file
        with open(conf.blocking_info_file_path, 'w') as f:
            for key in block_dict.keys():
                f.write(str(key) + ' ')
                for val in block_dict[key]:
                    f.write(str(val) + ' ')
                f.write("\n")

        threadLock.release()

    # print function is to print the current status of this site
    # print is a local operation
    def exePrint(self, variable):
        global log, block_dict, states

        if variable == "log":
            for eR in log:
                print "[Site " + str(eR.my_id) + "] [" + eR.operation + "] [" + str(eR.time) + "]: " + str(eR.content)
        elif variable == "block_dict":
            for record in block_dict.items():
                print "Site", record[0], "blocked sites:", [target_id for target_id in record[1]]
        elif variable == "states":
            print states
        elif variable == "partial_log":
            for eR in partial_log.items():
                print "[Site " + str(eR[1].my_id) + "] [" + eR[1].operation + "] [" + str(eR[1].time) + "]: " + str(
                        eR[1].content)


# --------------------------------------------------------------------------------
def quit_now(signum, frame):
    print 'You choose to stop me.'
    sys.exit()


def main():
    global id_addr, states, block_dict, MY_ID, MY_IP, MY_TZ, my_clock, timezone
    global MY_PORT, blocked_by, block_to, log, partial_log

    # load functions here to read data from disks

    # reading ips.txt
    site_id = MY_ID = int(raw_input("what is my twitter id?\n"))

    with open(conf.id_ports, 'r') as f:
        for ips in f:
            ips = ips.split()
            id_addr[int(ips[0])] = ips[1]+':'+ips[2]
            if MY_ID == int(ips[0]):
                MY_PORT = int(ips[2])
                MY_IP = ips[1]
                MY_TZ = ips[3]
                timezone = pytz.timezone(ips[3])

    if os.path.exists(conf.state_file_path):
        with open(conf.state_file_path, 'r') as f:
            for line in f.readlines():
                states = json.loads(line)["states"]
            my_clock = states[MY_ID][MY_ID]
    else:
        num = len(id_addr)
        my_clock = 0
        states = [[0 for i in range(num)] for i in range(num)]

    if os.path.exists(conf.blocking_info_file_path) and os.path.getsize(conf.blocking_info_file_path):
        with open(conf.blocking_info_file_path, 'r') as f:
            for line in f.readlines():
                if len(line) != line.count('\n'):
                    ids = line.split(' ', 1)
                    peer = int(ids[0])
                    bb = [int(x) for x in ids[1].split()]
                    block_dict[peer] = set(bb)
                    if MY_ID in block_dict[peer]:
                        blocked_by.add(peer)
                    if MY_ID == peer:
                        block_to = copy.copy(block_dict[peer])
    for peer_id in id_addr.keys():
        if peer_id not in block_dict:
            block_dict[peer_id] = set()

    if os.path.exists(conf.log_file_path):
        with open(conf.log_file_path, 'r') as f:
            for line in f.readlines():
                log_info = json.loads(line)
                datetime_struct = parser.parse(log_info["time"])
                event = Event(int(log_info["id"]), int(log_info["clock"]), log_info["operation"], datetime_struct, log_info["content"])
                log.append(event)

    if os.path.exists(conf.partial_log_file_path):
        with open(conf.partial_log_file_path, 'r') as f:
            for line in f.readlines():
                log_info = json.loads(line)
                datetime_struct = parser.parse(log_info["time"])
                event = Event(int(log_info["id"]), int(log_info["clock"]), log_info["operation"], datetime_struct,
                              log_info["content"])
                partial_log[event.my_clock*100+event.my_id] = event

    print MY_ID, "    ", MY_IP, ':', MY_PORT, " : ", timezone
    print 'start clock is : ', my_clock
    print 'id_addr: ', id_addr
    print 'states: ', states
    print 'log: ', log
    print 'partial_log: ', partial_log
    print 'block_dict: ', block_dict
    print 'block_to: ', block_to
    print 'blocked_by: ', blocked_by

    # Create new threads
    try:
        signal.signal(signal.SIGINT, quit_now)
        signal.signal(signal.SIGTERM, quit_now)
        # Start new Threads
        local = LocalThread(1, "Local Thread", "to handle local commands from terminal")
        remote = RemoteThread(2, "Remote Thread", "to receive messages from other sites")
        local.setDaemon(True)
        remote.setDaemon(True)
        threads.append(local)
        threads.append(remote)
        local.start()
        remote.start()

        while True:
            pass
    except Exception, exc:
        print exc


if __name__ == "__main__":
    main()


