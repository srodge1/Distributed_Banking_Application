import sys
import time
import socket as _sock
import logging as _log
import threading as _thr
from random import randint as _rint

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')

import bank_pb2
from google.protobuf.internal.encoder import _VarintEncoder
from google.protobuf.internal.decoder import _DecodeVarint


MAX_INTERVAL = 0     # maximum interval for sending money set at the start
sending_cons = {}    # stores a connection object for each branch, cur_branch is connected to, used only for sending msgs
                     # ..... can be assigned inside the class
counter      = None  # None:no snap_id; 0: stop transfer_msg and send marker_msgs; >=1: marker_msgs sent start listening
                     # ..... on receiving channels and record in snapshot data structure
console_format_str = "[%(threadName)s] %(message)s"

def encode_varint(value):
    """ Encode an int as a protobuf varint """
    data = []
    _VarintEncoder()(data.append, value, False)
    return b''.join(data)


def decode_varint(data):
    """ Decode a protobuf varint to an int """
    return _DecodeVarint(data, 0)[0]

class Branch():
    def __init__(self, b_balance=0, b_name=None, b_ip=None, b_port=0):
        self.balance, self.name, self.ip, self.port, self.lock = 0, b_name, b_ip, b_port, _thr.Lock()
        self.c_lock, self.s_lock = _thr.Lock(), _thr.Lock() # for counter and snapshot structure
        self.branch_dict = {}    # branch_name : Branch() object
        self.branch_snap = {}    # {snap_id : {bal, multiple channel_states}} for storing local snapshot
        self.branch_list = []    # branch_name list of all branches except current one
        self.num_branch  = 0     # len(branch_list) ; no. of branches this branch is connected to
        self.s_id        = None  # snap_id of the most recent snapshot. easier access to snap_id. No lock needed
                         #...since updated in init_snapshot, read otherwise. cont.. waits till snapshot algo converges
        self.init_snap, self.ready, self.sent, self.log = False, False, True, False
        self.mark_branch = []

    def __str__(self):
        return "NAME: {} ; BAL: {} at IP: {} ; PORT: {}".format(cur_branch.name, cur_branch.balance,
                    cur_branch.ip, cur_branch.port)

    def set_balance(self, b_balance):
        """ sets current branch's balance """
        self.balance = b_balance

    def get_balance(self):
        """ returns current branch's balance """
        return self.balance

    def add_balance(self, amt):
        """ adds money to the current branch balance """
        self.balance += amt

    def remove_balance(self, amt):
        """ removes money from current branch balance """
        self.balance -= amt

def handle_sending():
    """ Sender threads that handles the transfer msgs. """
    global counter
    while True:
        # counter is changed frm None to 0 on receiving InitSnapshot or first marker, and thus should stop transfers
        time.sleep((_rint(0, MAX_INTERVAL))/1000.0) # /1000 for seconds
        cur_branch.c_lock.acquire()
        if not cur_branch.ready and cur_branch.sent:
            # print("\n----in send tran counter : {} ; at {} --------".format(counter, cur_branch.name))
            with cur_branch.lock:
                amt = (_rint(1, 5)*cur_branch.balance)//100
                if (cur_branch.balance - amt) >= 0:
                    s_msg = bank_pb2.BranchMessage()
                    s_msg.transfer.src_branch = cur_branch.name
                    s_msg.transfer.money      = amt
                    cur_branch.balance -= amt
                    # select dest branch randomly #### **** following code can be written outside the lock?
                    dst_branch                = cur_branch.branch_list[_rint(0, (cur_branch.num_branch-1))]
                    s_msg.transfer.dst_branch = dst_branch
                    # getting the connection for destination branch from sending_cons
                    # print("\nsending tran s {}; d {}; m {} ; c {} ; cm {}".format(s_msg.transfer.src_branch,
                    #    s_msg.transfer.dst_branch, s_msg.transfer.money, cur_branch.name, cur_branch.balance))
                    s_msg = s_msg.SerializeToString()
                    size = encode_varint(len(s_msg))
                    s_socket = _sock.create_connection((cur_branch.branch_dict[dst_branch].ip,
                                                cur_branch.branch_dict[dst_branch].port))
                    s_socket.sendall(size+s_msg)
                    s_socket.close()
                    if cur_branch.log:
                        b_logger.debug(" transferred ${} to {}. updated balance = ${}".format(amt, dst_branch,
                                        cur_branch.balance))
                    # remove the amount that is sent to dest branch from current branch # if failed retain money
                    cur_branch.c_lock.release()
                # maybe log this event. work on this later
                else:
                    cur_branch.c_lock.release()
        else:
            # counter set to 0 means either the first marker msg or init_snapshot msg received
            if cur_branch.ready and not cur_branch.sent:
                #print("\n\t\t----in send mark counter 0 : {} ; at {} --------".format(counter, cur_branch.name))
                # start sending marker msgs to other branches, = 1 ; but after increment only in received marker msg
                m_msg = bank_pb2.BranchMessage()
                m_msg.marker.snapshot_id = cur_branch.s_id
                m_msg.marker.src_branch = cur_branch.name
                for _ in cur_branch.branch_dict:
                    if _ != cur_branch.name:
                        m_msg.marker.dst_branch = _
                        sm_msg = m_msg.SerializeToString()
                        size = encode_varint(len(sm_msg))
                        s_socket = _sock.create_connection((cur_branch.branch_dict[_].ip,
                                                    cur_branch.branch_dict[_].port))
                        s_socket.sendall(size+sm_msg)
                        s_socket.close()
                        #print('\n marker sent to : {} ; from {}'.format(_, cur_branch.name))
                # increment counter after sending marker msgs
                counter += 1
                with cur_branch.s_lock:
                    if len(cur_branch.mark_branch) == (cur_branch.num_branch):
                        counter = None
                        cur_branch.mark_branch = []
                cur_branch.ready = False
                cur_branch.sent  = True
                cur_branch.c_lock.release()
            else:
                cur_branch.c_lock.release()


def handle_msgs(client, addr):
    """
    Handles InitBranch msg, transfers, snapshots, marker msgs etc
    input : client(a socket obj), addr(address of remote conn)
    """
    global counter
    # loop so that threads created by sender won't die and keep receiving. for sending_cons.
    while True:
        data = b''
        while True:
            try:
                data += client.recv(1)
                size = decode_varint(data)
                break
            except IndexError:
                pass

        # if recv keeps throughing error. use data=[], in loop append to data received msg. Then ''.join(data)
        data = []
        while True:
            try:
                msg  = client.recv(size)
                data.append(msg)
                msg  = ''.join(data)
                init_msg = bank_pb2.BranchMessage()
                init_msg.ParseFromString(msg)
                break
            except:
                pass

        if init_msg.HasField('init_branch'):
            # setting up the all_branches_list and baance for this branch from init_msg
            cur_branch.c_lock.acquire()
            with cur_branch.lock:
                cur_branch.balance += init_msg.init_branch.balance
            for each in init_msg.init_branch.all_branches:
                if each.name not in cur_branch.branch_dict:
                    branch = Branch()
                    branch.name = each.name
                    branch.ip   = each.ip
                    branch.port = each.port
                    cur_branch.branch_dict[each.name] = branch
                    # TCP provides in-order delivery, if data is duplicated that means buffered data is received
                else:
                    break
            # keep this connection alive as it is needed by the snapshot msgs
            client.close()
            time.sleep(6) # in case other branches haven't received the init msg wait
            # creating the connections with all the branches. Every branch mentains connections seperately for sending
            #for _ in cur_branch.branch_dict:
            #    if _ != cur_branch.name:
            #        s_socket = _sock.create_connection((cur_branch.branch_dict[_].ip, cur_branch.branch_dict[_].port))
            #        sending_cons[_] = s_socket
                    #print("{} at {}".format(s_socket, cur_branch.port))
            # creating list of all the branches. ##### this list doesn't includes itself
            cur_branch.branch_list = [each for each in cur_branch.branch_dict.keys() if each != cur_branch.name]
            cur_branch.num_branch  = len(cur_branch.branch_list)
            #print('\n----- {} -----\n'.format(cur_branch.branch_list))

            th  = _thr.Thread(target=handle_sending, name=('st_'+cur_branch.name))
            th.start()
            # since sender_thread should be only one and branch_msg is sent only once. This portion is executed only once
            # .... and kills only the thread for init_branch by controller
            cur_branch.c_lock.release()
            client.close()
            break

        elif init_msg.HasField('transfer'):
            # on receiving money use the same lock to updte the balance.
            cur_branch.c_lock.acquire()
            #print("\nrecv tran s {}; d {}; m {} ; c {} ; cm {}".format(init_msg.transfer.src_branch,
            #        init_msg.transfer.dst_branch, init_msg.transfer.money, cur_branch.name, cur_branch.balance))
            if counter != None and init_msg.transfer.src_branch not in cur_branch.mark_branch:
                chan_state = init_msg.transfer.src_branch+'->'+init_msg.transfer.dst_branch
                with cur_branch.s_lock:
                    try:
                        cur_branch.branch_snap[cur_branch.s_id][chan_state] += init_msg.transfer.money
                        #print("this happens")
                    except KeyError:
                        cur_branch.branch_snap[cur_branch.s_id][chan_state] = init_msg.transfer.money

            if init_msg.transfer.dst_branch == cur_branch.name:
                with cur_branch.lock:
                    cur_branch.balance += init_msg.transfer.money
                    if cur_branch.log:
                        b_logger.debug(" received ${} from {}. updated balance = ${}.".format(init_msg.transfer.money,
                                        init_msg.transfer.src_branch, cur_branch.balance))
            cur_branch.c_lock.release()
            client.close()
            break
            #print("\ns {}; d {}; m {} ; c {} ; cm {}".format(init_msg.transfer.src_branch, init_msg.transfer.dst_branch,
            #        init_msg.transfer.money, cur_branch.name, cur_branch.balance))

        elif init_msg.HasField('init_snapshot'):
            # record the current balance into snapshot data structure ## assumes snap_id to be unique
            cur_branch.c_lock.acquire()
            cur_branch.ready = True
            cur_branch.sent  = False
            with cur_branch.s_lock:
                cur_branch.init_snap = True
                cur_branch.s_id = init_msg.init_snapshot.snapshot_id
                cur_branch.branch_snap[cur_branch.s_id] = {}
                #print('\n\tinit snap recv at {} with sid:{}'.format(cur_branch.name,
                #        init_msg.init_snapshot.snapshot_id))
                with cur_branch.lock:
                    cur_branch.branch_snap[cur_branch.s_id][cur_branch.name] = cur_branch.balance
                    #print("\nsid: {}; bn: {}; b: {}".format(cur_branch.s_id, cur_branch.name, cur_branch.balance))
            # stops the transfer_msg and starts sending marker_msg to other branches

            counter = 0
            cur_branch.c_lock.release()
            #break # kills the thread from controller. to keep alive store conns at controller and change implementation

        elif init_msg.HasField('marker'):
            #with cur_branch.c_lock:
                #print("\n----in recv marker counter : {} f : {} t : {} sid: {}----".format(counter, init_msg.marker.src_branch,
                #                                        cur_branch.name, init_msg.marker.snapshot_id))
            # on receiving increment counter; if it is the first msg with that snap_id counter=0 and store the local bal
            #... inside the cur_branch snap data structure
            #time.sleep(2)
            with cur_branch.c_lock:
                # if this is the first marker_msg counter=0; save current state and sender->recv : {empty};channel state
                if counter is None and init_msg.marker.snapshot_id not in cur_branch.branch_snap:
                    counter = 0
                    cur_branch.ready = True
                    cur_branch.sent  = False
                    # creating the entry for snap_id in snapshot data structure
                    with cur_branch.s_lock:
                        cur_branch.s_id = init_msg.marker.snapshot_id
                        cur_branch.branch_snap[cur_branch.s_id] = {}
                        cur_branch.mark_branch.append(init_msg.marker.src_branch)
                        #print('\n\tmarker recv at {} from {} with sid : {}'.format(cur_branch.name, init_msg.marker.src_branch,
                        #            init_msg.marker.snapshot_id))
                        with cur_branch.lock:
                            cur_branch.branch_snap[cur_branch.s_id][cur_branch.name] = cur_branch.balance

                        chan_state = init_msg.marker.src_branch+'->'+init_msg.marker.dst_branch
                        cur_branch.branch_snap[cur_branch.s_id][chan_state] = 0

                elif counter != None and init_msg.marker.src_branch not in cur_branch.mark_branch:
                    with cur_branch.s_lock:
                        cur_branch.mark_branch.append(init_msg.marker.src_branch)
                        if (len(cur_branch.mark_branch) == (cur_branch.num_branch)) and cur_branch.sent:
                            counter = None
                            cur_branch.mark_branch = []

            client.close()
            break
                # elif counter == 0 should be handled by sending thread i.e. by sending marker_msgs to everyone and incre
                #.... menting counter to 1.

        elif init_msg.HasField('retrieve_snapshot'):
            id = init_msg.retrieve_snapshot.snapshot_id
            #print('\nin ret snap : id : {}'.format(id))
            r_snap_msg = bank_pb2.BranchMessage()
            r_snap_msg.return_snapshot.local_snapshot.snapshot_id = id
            with cur_branch.s_lock:
                chan_stat = []
                for _ in cur_branch.branch_snap[id]:
                    if _ != cur_branch.name:
                        chan_stat.append(cur_branch.branch_snap[id][_])
                    else:
                        r_snap_msg.return_snapshot.local_snapshot.balance = cur_branch.branch_snap[id][_]
                # repeated fields don't have add() nor it permits assignment
                r_snap_msg.return_snapshot.local_snapshot.channel_state.extend(chan_stat)

            r_snap_msg = r_snap_msg.SerializeToString()
            size = encode_varint(len(r_snap_msg))
            client.sendall(size+r_snap_msg)

if __name__ == '__main__':
    MAX_INTERVAL = int(sys.argv[3])
    # creating instance of this branch/ cur_branch and assigning the balance, ip, port and name
    cur_branch = Branch()
    cur_branch.name    = sys.argv[1]
    cur_branch.port    = int(sys.argv[2])
    cur_branch.ip      = _sock.gethostbyname(_sock.gethostname())
    ##############
    if MAX_INTERVAL >= 1000:
        cur_branch.log = True
        b_logger           = _log.getLogger(cur_branch.name)
        console_handler    = _log.StreamHandler(sys.stdout)
        console_handler.setFormatter(_log.Formatter(console_format_str))
        b_logger.addHandler(console_handler)
        b_logger.setLevel(_log.DEBUG)
    ##############
    # create a socket and listen and print initially
    # since it is TCP i'm using SOCK_STREAM. for UDP use SOCK_DGRAM. the protocol is defaulted to 0(3rd parameter).
    l_socket = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    l_socket.bind(('', int(sys.argv[2])))
    l_socket.listen(5)
    # Until init msg is received main thread is blocked here.
    while True:
        client, addr = l_socket.accept()
        # Creating a thread to handle each received message
        th  = _thr.Thread(target=handle_msgs, args=(client, addr), name=('rt_'+cur_branch.name))
        th.daemon = True
        th.start()
