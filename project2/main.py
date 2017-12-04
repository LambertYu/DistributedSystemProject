import socket
import threading
import Queue
import cPickle as pickle
import time
from datetime import datetime
import os

# mutex when read&write in-memory or disk
thread_lock  = threading.Lock()
file_lock    = threading.Lock()

# config data of other sites
id_addr = {}                  # {site_id: (ip address, port number)}
sender   = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# related data of local site
tweets = []                   # [tweet logs]
block_dict = {}               # {site_id: set of bloked site_id}
logs = {}                     # {log_id: log content}
site_id = -1                  # site_id of local site

# related data of Paxos
waiting_queue = Queue.Queue() # waiting queue of logs that need to reach consensus
check_hole_period = 10        # check log holes for every 60 seconds
largest_log_id = -1           # largest commited log_id in logs
last_checked_logid = -1       # max log_id already checked last time
request_logid_max = -1        # the max log_id that received from other sites
prepare_dict = {}             # {log_id: [max_prepare, acc_proposalId, acc_event]}

request_logid_count = 0       # number of sites that respond their max log_id
prepare_ack_count = 0         # number of sites that respond the prepare()
accept_ack_count = 0          # number of sites that confirm the accept()

# log data of every event
class Event(object):
    def __init__(self, site_id, time, operation, content):
        self.log_id = -1
        self.site_id = site_id
        self.time = time
        self.operation = operation
        self.content = content

# ReceiveThread function is to receive messages from other sites
# and take different actions based the received message
class ReceiveThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        # continuously receive message from UDP socket
        while True:
            message, addr = receiver.recvfrom(1024)
            data = pickle.loads(message)

            if data[0] == "requestLogid":
                self.receiveRequestLogid(data)
            elif data[0] == "requestLogidAck":
                self.receiveRequestLogidAck(data)
            elif data[0] == "prepare":
                self.receivePrepare(data)
            elif data[0] == "prepareAck":
                self.receivePrepareAck(data)
            elif data[0] == "accept":
                self.receiveAccept(data)
            elif data[0] == "acceptAck":
                self.receiveAcceptAck(data)
            elif data[0] == "commit":
                self.receiveCommit(data)

    # -- Acceptor Role --
    # return the max log_id of current site
    # data: ["requestLogid", target_site_id]
    def receiveRequestLogid(self, data):
        global largest_log_id, id_addr

        msg = ["requestLogidAck", largest_log_id]
        message = pickle.dumps(msg)
        sender.sendto(message, id_addr[data[1]])

    # -- Receiver Role --
    # update the max log_id received from other sites
    # data: ["requestLogidAck", largest_log_id]
    def receiveRequestLogidAck(self, data):
        global request_logid_max, request_logid_count

        # update max logid & counter
        request_logid_max = max(request_logid_max, data[1])
        request_logid_count += 1

    # -- Acceptor Role --
    # update prepare_dict
    # send prepare_ack(): log_id + acc_num + acc_val
    # data: ["prepare", log_id, proposal_id, site_id]
    def receivePrepare(self, data):
        global prepare_dict, prepare_ack_count, id_addr

        log_id = data[1]
        proposal_id = data[2]

        if log_id not in prepare_dict:
            prepare_dict[log_id] = [-1, -1, -1]

        if proposal_id > prepare_dict[log_id][0]:
            prepare_dict[log_id][0] = proposal_id
            prepare_ack_count += 1

            msg = ["prepareAck", log_id, prepare_dict[log_id][1], prepare_dict[log_id][2]]
            message = pickle.dumps(msg)
            sender.sendto(message, id_addr[data[3]])

    # -- Receiver Role --
    # update prepare_dict & prepare_ack_count
    # data: ["prepareAck", log_id, acc_proposal, acc_event]
    def receivePrepareAck(self, data):
        global prepare_dict, prepare_ack_count

        if data[2] > prepare_dict[data[1]][1]:
            prepare_dict[data[1]][1] = data[2]  # acc_proposal_id
            prepare_dict[data[1]][2] = data[3]  # acc_event

        prepare_ack_count += 1

    # -- Acceptor Role --
    # compare with current max_prepare & send accept_ack()
    # data: ["accept", log_id, proposal_id, acc_event, site_id]
    def receiveAccept(self, data):
        global prepare_dict, id_addr

        if data[2] >= prepare_dict[data[1]][0]:
            prepare_dict[data[1]][0] = data[2]
            prepare_dict[data[1]][1] = data[2]
            prepare_dict[data[1]][2] = data[3]

            msg = ["acceptAck", data[1], data[2], data[3]]
            message = pickle.dumps(msg)
            sender.sendto(message, id_addr[data[4]])

    # -- Proposer Role --
    # send commit()
    # data: ["acceptAck", log_id, proposal_id, acc_event]
    def receiveAcceptAck(self, data):
        global accept_ack_count

        accept_ack_count += 1

    # -- Acceptor Role --
    # update local logs & write to disk
    # data: ["commnit", log_id, acc_event]
    def receiveCommit(self, data):
        global logs, tweets, thread_lock, file_lock, block_dict, site_id
        global waiting_queue, largest_log_id, last_checked_logid, request_logid_max, prepare_dict

        thread_lock.acquire()

        # update logs
        logs[data[1]] = data[2]
        largest_log_id = max(largest_log_id, data[1])
        e = data[2]

        # update other state variables
        if e.operation == "tweet":
            tweets.append(data[2])

        elif e.operation == "block":
            if e.site_id in block_dict.keys():
                block_dict[e.site_id].add(e.content)

            # remove the tweets issued by the site blocked you
            if e.content == site_id:
                tweets2 = []
                for t in tweets:
                    if t.site_id != e.site_id:
                        tweets2.append(t)
                tweets = tweets2

        elif e.operation == "unblock":
            if e.site_id in block_dict.keys() and e.content in block_dict[e.site_id]:
                block_dict[e.site_id].remove(e.content)

            # add the tweets issued by the site unblocked you
            if e.content == site_id:
                for t in logs:
                    if t.operation == "tweet" and t.site_id == e.site_id:
                        tweets.append(t)

        # write to disk
        file_lock.acquire()

        pickle.dump(logs, open("logs.dat", "wb"))
        paxos = [largest_log_id, last_checked_logid, request_logid_max, prepare_dict]
        pickle.dump(paxos, open("paxos.dat", "wb"))

        file_lock.release()
        thread_lock.release()

# CheckHoleThread function is to check holes in logs every 60 seconds
class CheckHoleThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global check_hole_period, logs, waiting_queue, last_checked_logid, request_logid_max

        while True:
            time.sleep(check_hole_period)

            for log_id in range(last_checked_logid + 1, request_logid_max + 1):
                if log_id in logs:
                    last_checked_logid = log_id
                    continue

                event = Event(0, 0, "", "")
                event.log_id = log_id
                waiting_queue.put(event)

                while (log_id not in logs):
                    time.sleep(1)
                last_checked_logid = log_id

# broadcast the events one by one from waiting_queue
class SynodQueueThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global waiting_queue, largest_log_id, logs, id_addr, sender
        global request_logid_max, prepare_dict
        global request_logid_count, prepare_ack_count, accept_ack_count

        is_need_get_from_queue = True
        event = Event(0, 0, "", "")
        while True:
            if is_need_get_from_queue:
                event = waiting_queue.get(True)

            # ---- "requestLogid" to get the max logid of other sites ----
            if event.log_id == -1:
                request_logid_max = largest_log_id
                request_logid_count = 1

                msg = ["requestLogid", site_id]
                message = pickle.dumps(msg)
                for target_id in range(1, 6):
                    if site_id == target_id:
                        continue
                    sender.sendto(message, id_addr[target_id])

                # wait until majority "requestLogidAck" received or timeout
                start = time.time()
                while (request_logid_count < 3 and time.time() - start < 15):
                    time.sleep(0.05)

                # get the log_id of current new event
                if (request_logid_count < 3):
                    event.log_id = -1
                    is_need_get_from_queue = False
                    continue

                event.log_id = request_logid_max + 1

            log_id = event.log_id

            # ---- send prepare(log_id, proposal_id) to other sites ----
            prepare_ack_count = 1
            prepare_dict[log_id] = [-1, -1, -1]  # [max_prepare_proposal_id, acc_proposal_id, acc_event]

            proposal_id = (datetime.utcnow() - datetime(1970,1,1)).total_seconds() * 10 + site_id  # timestamp + site_id
            msg = ["prepare", log_id, proposal_id, site_id]
            message = pickle.dumps(msg)
            for target_id in range(1, 6):
                if site_id == target_id:
                    continue
                sender.sendto(message, id_addr[target_id])

            # block until majority respond or timeout
            # choose value & send accept(): largest acc_num & its acc_val
            start = time.time()
            while (prepare_ack_count < 3 and time.time() - start < 15):
                time.sleep(0.05)

            if (prepare_ack_count < 3):
                event.log_id = -1
                is_need_get_from_queue = False
                continue

            # choose own value(event) when acc_proposal_id is null(-1)
            if (prepare_dict[log_id][2] == -1):
                prepare_dict[log_id][2] = event

            # ---- send "accept" to other sites ----
            accept_ack_count = 0
            msg = ["accept", log_id, proposal_id, prepare_dict[log_id][2], site_id]
            message = pickle.dumps(msg)
            for target_id in range(1, 6):
                sender.sendto(message, id_addr[target_id])

            start = time.time()
            while (accept_ack_count < 3 and time.time() - start < 15):
                time.sleep(0.05)

            if (accept_ack_count < 3):
                event.log_id = -1
                is_need_get_from_queue = False
                continue

            # ---- send "commit" to other sites ----
            msg = ["commit", log_id, prepare_dict[log_id][2]]
            message = pickle.dumps(msg)
            for target_id in range(1, 6):
                sender.sendto(message, id_addr[target_id])

            is_need_get_from_queue = True

# read commands from terminal & take different actions then
class TerminalThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

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
            if operation == "tweet" and content != "":
                self.tweetFunc(content)
            elif operation == "view":
                self.viewFunc()
            elif operation == "block" and content != "":
                self.blockFunc(int(content))
            elif operation == "unblock" and content != "":
                self.unblockFunc(int(content))
            elif operation == "print":
                self.printFunc(content)
            elif operation == "help":
                print "tweet Hello, world!\nview\nblock 1\nunblock 2\nprint logs/block_dict"

    # tweetFunc is to post a tweet by this site
    def tweetFunc(self, content):
        global waiting_queue

        event = Event(site_id, datetime.utcnow(), "tweet", content)
        waiting_queue.put(event)

    # viewFunc is to view all tweets from unblocked sites
    def viewFunc(self):
        global tweets, thread_lock

        thread_lock.acquire()

        # sort the tweets
        tweets.sort(key = lambda event: event.time, reverse = True)

        # print the tweets following a certain format
        for eR in tweets:
            if eR.site_id not in block_dict[site_id]:
                print "Site:", eR.site_id, "posted at", str(eR.time), ":", eR.content

        thread_lock.release()

    # blockFunc is to handle block commands
    def blockFunc(self, target_id):
        global waiting_queue, block_dict, site_id

        # check the validation of target_id
        if target_id == site_id:
            print "[Invalid Id] You can not block yourself"
            return

        if target_id not in id_addr.keys():
            print "[Invalid Id] There is no site with id:", target_id
            return

        if target_id in block_dict[site_id]:
            print "[Invalid Id] This site is already blocked"
            return

        event = Event(site_id, datetime.utcnow(), "block", target_id)
        waiting_queue.put(event)

    # unblockFunc is to handle unblock commands
    def unblockFunc(self, target_id):
        global block_dict, waiting_queue, site_id

        # check the validation of target_id
        if target_id not in block_dict.keys():
            print "[Invalid Id] There is no site with id:", target_id
            return

        if target_id not in block_dict[site_id]:
            print "[Invalid Id] This site hasn't been blocked before!"
            print "Current blocked sites are:", [block_id for block_id in block_dict[site_id]]
            return

        event = Event(site_id, datetime.utcnow(), "unblock", target_id)
        waiting_queue.put(event)

    # print function is to print the current status of this site
    def printFunc(self, content):
        global block_dict, logs, prepare_dict, last_checked_logid, logs

        thread_lock.acquire()

        if content == "block_dict":
            for id in block_dict.keys():
                print "[ site", id, "] [ block sites ]:", [blocked for blocked in block_dict[id]]
        elif content == "logs":
            for log_id in logs.keys():
                e = logs[log_id]
                print "[ log", log_id, "] [ site", e.site_id, "] [", e.operation, "] [", str(e.time), "]:", str(e.content)
        elif content == "prepare_dict":
            for log_id in prepare_dict.keys():
                print "[ log", log_id, "] [max_prepare, acc_proposalId, acc_event]: ", prepare_dict[log_id]
        elif content == "last_checked_logid":
            print "[ last_checked_logid ]", last_checked_logid

        thread_lock.release()

# load state variables from disk
# replay the log to recover tweets & block_dict
def recoverVariables():
    global logs, tweets, block_dict, site_id, id_addr, file_lock
    global waiting_queue, largest_log_id, last_checked_logid, request_logid_max, prepare_dict

    file_lock.acquire()

    # load logs
    if os.path.isfile("logs.dat"):
        logs = pickle.load(open("logs.dat", "rb"))

    # load Paxos related variables
    if os.path.isfile("paxos.dat"):
        paxos = pickle.load(open("paxos.dat", "rb"))
        largest_log_id = paxos[0]
        last_checked_logid = paxos[1]
        request_logid_max = paxos[2]
        prepare_dict = paxos[3]

    # recover tweets & block_dict by replaying logs
    for log_id in range(largest_log_id + 1):
        # if missing logs, push to waiting_queue for recover
        if log_id not in logs.keys():
            event = Event(site_id, 0, "", "")
            event.log_id = log_id
            waiting_queue.put(event)
            continue

        # update tweets & block_dict
        e = logs[log_id]
        if e.operation == "tweet":
            tweets.append(e)
        elif e.operation == "block":
            block_dict[e.site_id].add(e.content)
        elif e.operation == "unblock":
            block_dict[e.site_id].remove(e.content)

    file_lock.release()

def main():
    global id_addr, site_id, receiver

    # load configuration from disk
    file = open("config.txt", "r")
    for line in file:
        # [site_id, ip_address, port_number, location_name, if_current_site]
        data = line.split()
        id_addr[int(data[0])] = (data[1], int(data[2]))
        # if this line is the config of local site
        if data[4] == "yes":
            site_id = int(data[0])
            # bind the current ip address to receiver UDP port
            receiver.bind(id_addr[site_id])

            print receiver
        # init the block_dict
        block_dict[int(data[0])] = set()
    file.close()

    # load state variables from disk & replay the log to recover
    recoverVariables()

    terminal = TerminalThread()
    receive_server = ReceiveThread()
    synod_queue = SynodQueueThread()
    check_hole = CheckHoleThread()

    receive_server.start()
    synod_queue.start()
    check_hole.start()
    terminal.start()

if __name__ == "__main__":
    main()



