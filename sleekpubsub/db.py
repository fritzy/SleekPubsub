import sqlite3

class PubsubDB(object):
	
	def __init__(self, file):
		self.conn = sqlite3.connect(file)
	
	def createNode(self, node, config, affiliations, items={}):
		c = self.conn.cursor()
		c.execute('insert into node (name, config) values(?,?)', (node, code.getXML()))
		c.close()
		self.conn.commit()
		self.synch(node, affiliations=affiliations, items=items)
	
	def hasNode(self, name):
		c = self.conn.cursor()
		c.execute('select id from node where name=?', (name,))
		r = c.fetchall()
		c.close()
		return bool(len(r))
	
	def synch(self, node, config=None, affiliations={}, items={}, subscriptions=[]):
		c = self.conn.cursor()
		c.execute('select id from node where name=?')
		id = c.fetchone()[0]
		update = []
		for aftype in self.affiliations:
			for jid in self.affiliations[aftype]:
				update.append((id, jid, aftype))
		c.execute('replace into affiliation (node_id, jid, type) values (?,?,?)', update)
		if config is not None:
			c.execute('update node set config=? where name=?', (config, node))
		update = [(id, item_name, items[item_name].getpayload(), items[item_name].gettime(), items[item_name].getwho()) for item_name in items]
		c.execute('replace in item (node_id, name, payload, time, who) values (?,?,?,?,?)', update)
		update = [(id, sub.getjid(), sub.getconfig(), sub.getid()) for sub in subscriptions]
		c.execute('replace in subscription (node_id, jid, config, subid) values (?,?,?,?)', update)
		self.conn.commit()
		c.close()
	
	def updateNodeConfig(self, node, config):
		self.synch(node, config=config)
	
	def delNode(self):
		pass
	
	def getNode(self):
		pass
	
	def getNodeConfig(self, node):
		c = self.conn.cursor()
		c.execute('select config from node where name=?', (node,))
		r = c.fetchone()
		return r[0]
	
	def getNodes(self):
		pass
	
	def setItem(self, item, limit=0):
		pass
	
	def delItem(self):
		pass
	
	def getItems(self, node):
		return {}
	
	def setAffiliation(self):
		pass
	
	def getAffiliations(self, node):
		aff = {'owner': [], 'publisher': [], 'member': [], 'outcast': [], 'pending': []}
		return aff
		#TODO pull this from the db

