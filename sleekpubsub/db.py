import sqlite3
import pickle
import threading
import sys
try:
	import queue
except ImportError:
	import Queue as queue

class PubsubDB(object):
	
	def __init__(self, file, xmpp):
		self.xmpp = xmpp
		self.file = file
		self.thread = threading.Thread(name='db_queue', target=self.dbStartWrite)
		self.threadr = threading.Thread(name='db_queue read', target=self.dbStartRead)
		self.win = queue.Queue()
		self.rin = queue.Queue()
		self.thread.start()
		self.threadr.start()
	
	def dbStartWrite(self):
		self.conn = sqlite3.connect(self.file)
		while True:
			reply, pointer, args = self.win.get(block=True)
			result = pointer(*args)
			if reply is not None:
				reply.put(result)
	
	def dbStartRead(self):
		self.rconn = sqlite3.connect(self.file)
		if sys.version_info < (3, 0):
			self.rconn.text_factory = str
		while True:
			reply, pointer, args = self.rin.get(block=True)
			result = pointer(*args)
			if reply is not None:
				reply.put(result)
	
	def getRoster(self):
		out = queue.Queue()
		self.rin.put((out, self._getRoster, tuple()))
		return out.get(block=True)

	def _getRoster(self):
		c = self.rconn.cursor()
		c.execute('select jid,jidto from roster where subto=1')
		result = [(x[0],x[1]) for x in c.fetchall()]
		c.close()
		return result
	
	def getRosterJid(self, ifrom):
		out = queue.Queue()
		self.rin.put((out, self._getRosterJid, (ifrom,)))
		return out.get(block=True)
	
	def _getRosterJid(self, ifrom):
		c = self.rconn.cursor()
		c.execute('select subto, subfrom from roster where jid=?', (ifrom,))
		result = c.fetchall()
		if not result:
			c.execute('insert into roster (jid, subto, subfrom) values (?,0,0)', (ifrom,))
			self.rconn.commit()
			c.close()
			return (0,0)
		else:
			c.close()
			return result[0]

	def setRosterTo(self, ifrom, value, ito):
		self.win.put((None, self._setRosterTo, (ifrom, value, ito)))

	def _setRosterTo(self, ifrom, value, ito):
		c = self.conn.cursor()
		c.execute('update roster set subto=?, jidto=? where jid=?', (int(value),ito,ifrom))
		self.conn.commit()
		c.close()
	
	def setRosterFrom(self, ifrom, value):
		self.win.put((None, self._setRosterFrom, (ifrom, value)))

	def _setRosterFrom(self, ifrom, value):
		c = self.conn.cursor()
		c.execute('update roster set subfrom=? where jid=?', (int(value),ifrom))
		self.conn.commit()
		c.close()
	
	def clearRoster(self, ifrom, value):
		self.win.put((None, self._clearRoster, (ifrom, value)))

	def _clearRoster(self, ifrom):
		c = self.conn.cursor()
		c.execute('update roster set subto=?, subfrom=?', (0,0))
		self.conn.commit()
		c.close()
	
	def deleteNode(self, node):
		self.win.put((None, self._deleteNode, (node,)))

	def _deleteNode(self, node):
		c = self.conn.cursor()
		c.execute("select id from node where name=?", (node,))
		id = c.fetchone()[0]
		c.execute('delete from node where id=?', (id,))
		c.execute('delete from subscription where node_id=?', (id,))
		c.execute('delete from affiliation where node_id=?', (id,))
		self.conn.commit()
		c.close()
	
	def createNode(self, node, config, affiliations, items={}):
		self.win.put((None, self._createNode, (node, config, affiliations, items)))
	
	def _createNode(self, node, config, affiliations, items={}):
		c = self.conn.cursor()
		c.execute('insert into node (name, config, type) values(?,?,?)', (node, pickle.dumps(config), config.get('pubsub#node_type', 'leaf')))
		c.close()
		self.conn.commit()
		self.synch(node, affiliations=affiliations, items=items)

	def hasNode(self, name):
		out = queue.Queue()
		self.rin.put((out, self._hasNode, (name,)))
		return out.get(block=True)

	def _hasNode(self, name):
		c = self.rconn.cursor()
		c.execute('select id from node where name=?', (name,))
		r = c.fetchall()
		c.close()
		return bool(len(r))
	
	
	def synch(self, node, config=None, affiliations={}, items={}, subscriptions=[]):
		self.win.put((None, self._synch, (node, config, affiliations, items, subscriptions)))

	def _synch(self, node, config=None, affiliations={}, items={}, subscriptions=[]):
		c = self.conn.cursor()
		c.execute('select id from node where name=?', (node,))
		id = c.fetchone()[0]
		updates = []
		for aftype in affiliations:
			for jid in affiliations[aftype]:
				c.execute('replace into affiliation (node_id, jid, type) values (?,?,?)', (id, jid, aftype))
		if config is not None:
			c.execute('update node set config=? where name=?', (config, node))
		updates = [(id, item_name, items[item_name].getpayload(), items[item_name].gettime(), items[item_name].getwho()) for item_name in items]
		for update in updates:
			c.execute('replace into item (node_id, name, payload, time, who) values (?,?,?,?,?)', update)
		updates = [(id, sub.getjid(), sub.getconfig(), sub.getid()) for sub in subscriptions]
		for update in updates:
			c.execute('replace into subscription (node_id, jid, config, subid) values (?,?,?,?)', update)
		self.conn.commit()
		c.close()
	
	def addSubscription(self, node, jid, subid, config=None, to=None):
		self.win.put((None, self._addSubscription, (node, jid, subid, config, to)))

	def _addSubscription(self, node, jid, subid, config=None, to=None):
		if config is not None:
			config = ET.tostring(config.getXML())
		c = self.conn.cursor()
		c.execute('select id from node where name=?', (node,))
		id = c.fetchone()[0]
		c.execute('insert into subscription (node_id, jid, config, subid, jidto) values (?,?,?,?,?)', (id, jid, config, subid, to))
		self.conn.commit()
		c.close()
	
	def updateNodeConfig(self, node, config):
		self.synch(node, config=config)
	
	def getSubscriptions(self, node):
		out = queue.Queue()
		self.rin.put((out, self._getSubscriptions, (node,)))
		return out.get(block=True)

	def _getSubscriptions(self, node):
		c = self.rconn.cursor()
		c.execute('select sub.jid, sub.subid, sub.config from node left join subscription as sub on sub.node_id=node.id where node.name=?', (node,))
		subs = []
		for row in c:
			if row[0] is not None:
				config = None
				if row[2] is not None:
					config = self.xmpp.plugin['xep_0004'].buildForm(ET.fromstring(row[2]))
				subs.append((row[0], row[1], config))
		return subs
	
	def delNode(self):
		pass
	
	def getNode(self):
		pass
	
	def getNodeConfig(self, node):
		out = queue.Queue()
		self.rin.put((out, self._getNodeConfig, (node,)))
		return out.get(block=True)

	def _getNodeConfig(self, node):
		c = self.rconn.cursor()
		c.execute('select config from node where name=?', (node,))
		r = c.fetchone()
		return r[0]
	
	def getNodes(self):
		out = queue.Queue()
		self.rin.put((out, self._getNodes, tuple()))
		return out.get(block=True)

	def _getNodes(self):
		c = self.rconn.cursor()
		c.execute('select name, type from node')
		return [row for row in c]
	
	def eachNode(self):
		c = self.conn.cursor()
		c.execute('select name, type from node')
		for row in c:
			yield row
		c.close()
	
	def setItem(self, node, item, payload):
		pass
	
	def delItem(self):
		pass
	
	def getItems(self, node):
		return {}
	
	def setAffiliation(self):
		pass
	
	def getAffiliations(self, node):
		out = queue.Queue()
		self.rin.put((out, self._getAffiliations, (node,)))
		return out.get(block=True)

	def _getAffiliations(self, node):
		aff = {'owner': [], 'publisher': [], 'member': [], 'outcast': [], 'pending': []}
		c = self.rconn.cursor()
		c.execute('select affiliation.jid, affiliation.type from node left join affiliation on affiliation.node_id=node.id where node.name=?', (node, ))
		for row in c:
			if row[0] is not None:
				aff[row[1]].append(row[0])
		c.close()
		return aff

