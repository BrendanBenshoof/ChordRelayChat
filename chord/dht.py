from chord import Local, Daemon, repeat_and_sleep, inrange
from remote import Remote
from address import Address
import json
import time
from threading import Thread
import time

G_DHT = None

def setup(port, remote):
	global G_DHT
	myaddr = Address("0.0.0.0",port)
	if remote is None or remote == 'None':
		remote = None
	else:
		data = remote.split(":")
		remote = Address(data[0],int(data[1]))

	G_DHT = DHT(myaddr, remote)

def getDHT():
	global G_DHT
	return G_DHT

def filter(list,test):
	output = []
	for i in list:
		if test(i):
			output.append(i)
	return output

# data structure that represents a distributed hash table
class DHT(object):
	def __init__(self, local_address, remote_address = None):
		self.local_ = Local(local_address, remote_address)
		def set_wrap(msg):
			return self._set(msg)
		def get_wrap(msg):
			return self._get(msg)

		def post_wrap(msg):
			return self._post(msg)
		def poll_wrap(msg):
			return self._poll(msg)

		self.data_ = {}
		self.channels = {}
		self.shutdown_ = False

		self.local_.register_command("set", set_wrap)
		self.local_.register_command("get", get_wrap)
		self.local_.register_command("post", post_wrap)
		self.local_.register_command("poll", poll_wrap)
		self.daemons_ = {}
		self.daemons_['distribute_data'] = Daemon(self, 'distribute_data')
		self.daemons_['distribute_data'].start()

		self.local_.start()

	def shutdown(self):
		self.local_.shutdown()
		self.shutdown_ = True

	def _get(self, request):
		try:
			data = json.loads(request)
			# we have the key
			return json.dumps({'status':'ok', 'data':self.get(data['key'])})
		except Exception:
			# key not present
			return json.dumps({'status':'failed'})

	def _poll(self, request):
		try:
			data = json.loads(request)
			key = data['key']
			time = data['time']
			# we have the key
			#print(("polling",key,time))
			return json.dumps({'status':'ok', 'data':self.pollChan(key,time)})
		except Exception:
			# key not present
			return json.dumps({'status':'failed'})

	def _post(self, request):
		try:
			data = json.loads(request)
			key = data['key']
			value = data['value']
			# we have the key
			self.postChan(key,value)
			return json.dumps({'status':'ok'})
		except Exception:
			# key not present
			return json.dumps({'status':'failed'})

	def _set(self, request):
		try:
			data = json.loads(request)
			key = data['key']
			value = data['value']
			self.set(key, value)
			return json.dumps({'status':'ok'})
		except Exception:
			# something is not working
			return json.dumps({'status':'failed'})

	def pollChan(self,key,timestamp):
		if key in self.channels.keys():
			tmp = self.channels[key]
			output = list(filter(tmp,lambda x: x[0] > timestamp))
			return output
		else:
			return None


	def postChan(self,key,msg):
		now = time.time()
		if key in list(self.channels.keys()):
			self.channels[key].append((now,msg))
		else:
			self.channels[key] = [(now,msg)]


	def get(self, key):
		try:
			return self.data_[key]
		except Exception:
			# not in our range
			suc = self.local_.find_successor(hash(key))
			if self.local_.id() == suc.id():
				# it's us but we don't have it
				return None
			try:
				response = suc.command('get %s' % json.dumps({'key':key}))
				if not response:
					raise Exception
				value = json.loads(response)
				if value['status'] != 'ok':
					raise Exception
				return value['data']
			except Exception:
				return None


	def post(self, key, value):
		# not in our range
		suc = self.local_.find_successor(hash(key))
		if self.local_.id() == suc.id():
			# it's us but we don't have it
			self.postChan(key,value)
			return True
		try:
			response = suc.command('post %s' % json.dumps({'key':key,'value':value}))
			if not response:
				raise Exception
			value = json.loads(response)
			if value['status'] != 'ok':
				raise Exception
			return True
		except Exception:
			return False


	def poll(self, key, time):
		#print("polling")
		suc = self.local_.find_successor(hash(key))
		if self.local_.id() == suc.id():
			return self.pollChan(key,time)
		else:
			# not in our range
			suc = self.local_.find_successor(hash(key))
			if self.local_.id() == suc.id():
				# it's us but we don't have it
				return None

			response = suc.command('poll %s' % json.dumps({'key':key,'time':time}))
			if not response:
				raise Exception
			value = json.loads(response)
			if value['status'] != 'ok':
				print(value)
			return value['data']
			"""
			except Exception as e:
				print(e)
				return None
			"""

	def set(self, key, value):
		# eventually it will distribute the keys
		self.data_[key] = value

	@repeat_and_sleep(5)
	def distribute_data(self):
		to_remove = []
		# to prevent from RTE in case data gets updated by other thread
		keys = list(self.data_.keys())
		for key in keys:
			if self.local_.predecessor() and \
			   not inrange(hash(key), self.local_.predecessor().id(1), self.local_.id(1)):
				try:
					node = self.local_.find_successor(hash(key))
					node.command("set %s" % json.dumps({'key':key, 'value':self.data_[key]}))
					# print "moved %s into %s" % (key, node.id())
					to_remove.append(key)
					print("migrated")
				except socket.error:
					print("error migrating")
					# we'll migrate it next time
					pass
		# remove all the keys we do not own any more
		for key in to_remove:
			del self.data_[key]
		# Keep calling us
		return True

def create_dht(lport):
	laddress = [Address('0.0.0.0', port) for port in lport]
	r = [DHT(laddress[0])]
	for address in laddress[1:]:
		r.append(DHT(address, laddress[0]))
	return r


class ChanWorker(Thread):
	def __init__(self,chanid,callback):
		Thread.__init__(self)
		self.chanid = chanid
		self.callback = callback
		self.DHT = getDHT()
		self.running = True

	def run(self):
		last_time = 0
		while(self.running):
			data = self.DHT.poll(self.chanid,last_time)
			if data:
				last_time = max([x[0] for x in data])
				for l in data:
					self.callback(l[1])
			time.sleep(1)

	def post(self, line):
		self.DHT.post(self.chanid,line)
