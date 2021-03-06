# -*- coding: utf-8 -*-

import time
from chord import dht

class ClientInChannel(object):
    
    def __init__(self, client):
        self.client = client
        self.operator = False
        self.voice = False
       
    
    def __str__(self):
        prefix = (self.operator and '@') or (self.voice and '%') or ''
        return '%s%s' % (prefix, self.client.nick)


class Channel(object):
    
    channels = {} # { chan_name : channel_obj }
    
    def __init__(self, name):
        self.local_lines = set([])
        self.name = name
        self.topic = 'Null'
        self.modes = set()
        self.scope_flag = '='  # @ is used for secret channels, * for private channels, = for others (public channels)
        self.clients = {} # { client : in_channel_property }
        self.owner = None
        self.key = None
        self.creation_date = int(time.time())
        self.chanworker = dht.ChanWorker(self.name, self.chanpost)
        self.chanworker.start()
        
    def add_client(self, client):
        self.clients[client] = ClientInChannel(client)
        
    def remove_client(self, client):
        del self.clients[client]

    def nicklist_to_string(self):
        nicklist = []
        for client_in_channel in self.clients.values():
            nicklist.append(str(client_in_channel))
        return ' '.join(nicklist)
    
    def relay(self, sender, line):
        self.local_lines.add(line)
        for client in self.clients:
            if not client == sender:
                client.send(line)
        self.chanworker.post(line)


    def chanpost(self,line):
        if line not in self.local_lines:
            print "chanpost", line
            for client in self.clients:
                client.send(line.encode('utf-8'))

            
