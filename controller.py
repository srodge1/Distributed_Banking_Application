from __future__ import print_function
import sys
import time
import socket as _sock
import logging as _log
import threading as _thr
from random import choice as _rch

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')

import bank_pb2
from google.protobuf.internal.encoder import _VarintEncoder
from google.protobuf.internal.decoder import _DecodeVarint


branches = {}     # branch : [ip, port]
connections = {}  # branch : connection object
branch_list = []
num_branches, snap_id = 0, 1
console_format_str = ''

def encode_varint(value):
    """ Encode an int as a protobuf varint """
    data = []
    _VarintEncoder()(data.append, value, False)
    return b''.join(data)


def decode_varint(data):
    """ Decode a protobuf varint to an int """
    return _DecodeVarint(data, 0)[0]

def handle_init(r_branch):
    global snap_id
    branch_msg = bank_pb2.BranchMessage()
    branch_msg.init_snapshot.snapshot_id = snap_id
    snap_id   += 1
    branch_msg = branch_msg.SerializeToString()
    size = encode_varint(len(branch_msg))
    connections[r_branch].sendall(size+branch_msg)

def handle_snapshots():
    sid = 1
    while True:
        # choose a branch at random from branch_list
        r_branch = _rch(branch_list)
        handle_init(r_branch)
        time.sleep(len(branch_list)*10)
        r_snap_msg = bank_pb2.BranchMessage()
        r_snap_msg.retrieve_snapshot.snapshot_id = sid
        r_snap_msg = r_snap_msg.SerializeToString()
        print('\n\nSnapshot id : {}'.format(sid))
        sid += 1

        for _ in connections:
            size = encode_varint(len(r_snap_msg))
            connections[_].sendall(size+r_snap_msg)
            data = b''
            while True:
                try:
                    data += connections[_].recv(1)
                    size = decode_varint(data)
                    break
                except IndexError:
                    pass
            msg = connections[_].recv(size)
            t_msg = bank_pb2.BranchMessage()
            t_msg.ParseFromString(msg)
            #for i in range(len(branch_list)-len(t_msg.return_snapshot.local_snapshot.channel_state)):
            #    t_msg.return_snapshot.local_snapshot.channel_state.append(0)
            print('\n{} : {},'.format(_, t_msg.return_snapshot.local_snapshot.balance), end=' ')
            chan_state = []
            p_str = ''
            l = [f[0].name for f in t_msg.return_snapshot.local_snapshot.ListFields()]
            if 'channel_state' not in l:
                p_str += ' '.join('{}->{} : {},'.format(each, _, 0) for each in branch_list if each != _)

            else:
                for f in t_msg.return_snapshot.local_snapshot.ListFields():
                    if f[0].name == 'channel_state':
                        for i in range(len(f[1])):
                            chan_state.append(f[1][i])
                        for i in range(len(branch_list)-len(chan_state)):
                            chan_state.append(0)
                        for i in range(len(branch_list)):
                            if branch_list[i] == _:
                                chan_state[-1] = chan_state[i]
                            else:
                                p_str += ' {}->{} : {},'.format(branch_list[i], _, chan_state[i])

            c_logger.debug(p_str[:-1])


if __name__ == '__main__':
    # read the file and get the ips and ports of all the branches to send the InitBranch msg
    try:
        with open(sys.argv[2], 'r') as f_handler:
            lines = f_handler.readlines()
            if len(lines) == 0:
                print('No data in this file! please try again!')
    except EnvironmentError:
        print('No such file exists. Please try again!')
        exit(-1)

    # distributing the balance evenly among the branches no. of lines = no. of branches
    branch_balance = int(sys.argv[1]) // len(lines)
    #print(lines)
    for line in lines:
        c_branch = line.split()
        s_socket = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        s_socket.bind(('', 0))

        c_ip, c_port = c_branch[1], int(c_branch[2])
        branches[c_branch[0]] = [c_ip]
        branches[c_branch[0]].append(c_port)
        # create_connection() higher-level fun than connect(). ip4/ipv6 compatible
        s_socket.connect((c_ip, c_port))
        # create a BranchMessage here and send it to the branches.......... Changes made to it's child class
        # will reflect in the parent class. protobuf official site ......... repeats has add() method.
        branch_msg   = bank_pb2.BranchMessage()
        # setting up the branch balances
        branch_msg.init_branch.balance  = branch_balance
        # adding all_branches field. repeats many times. find another way
        # at least 2 branches are assumed
        for each in lines:
            branch        = each.split()
            t_branch      = branch_msg.init_branch.all_branches.add()
            t_branch.name = branch[0]
            t_branch.ip   = branch[1]
            t_branch.port = int(branch[2].rstrip('\n'))

        # serializing the init msg t send on the socket
        serial_branch_msg   = branch_msg.SerializeToString()
        size = encode_varint(len(serial_branch_msg))
        # sending the serialized init messages to all the branches using raw socket
        while True:
            try:
                s_socket.sendall(size+serial_branch_msg)
            except _sock.error:
                break

        # close() doesn't stop sending right when called. so the receiver gets buffered unnecessary data
        # shutdown() probably will solve this issue. testing now. or use flush() if available
        #s_socket.shutdown(_sock.SHUT_RDWR)
        s_socket.close()
    branch_list = list(branches.keys())
    num_branches = len(branch_list)
    ####
    c_logger           = _log.getLogger('controller')
    console_handler    = _log.StreamHandler(sys.stdout)
    console_handler.setFormatter(_log.Formatter(console_format_str))
    c_logger.addHandler(console_handler)
    c_logger.setLevel(_log.DEBUG)
    ####
    time.sleep(10) # remove later
    # creating connections with all the branches
    for _ in branches:
        client = _sock.create_connection((branches[_][0], branches[_][1]))
        connections[_] = client
    handle_snapshots()
