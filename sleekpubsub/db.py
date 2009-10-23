import sqlite3
from xml.etree import cElementTree as ET

class PubsubDB(object):
	
	def __init__(self, file, xmpp):
		self.xmpp = xmpp
		self.conn = sqlite3.connect(file)
	
	def createNode(self, node, config, affiliations, items={}):
		values = config.getValues()
		c = self.conn.cursor()
		c.execute('insert into node (name, config, type) values(?,?,?)', (node, ET.tostring(config.getXML()), values.get('pubsub#node_type', 'leaf')))
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
		c.execute('select id from node where name=?', (node,))
		id = c.fetchone()[0]
		updates = []
		for aftype in affiliations:
			for jid in affiliations[aftype]:
				c.execute('replace into affiliation (node_id, jid, type) values (?,?,?)', (id, jid, aftype))
		if config is not None:
			c.execute('update node set config=? where name=?', (ET.tostring(config.getXML('form')), node))
		updates = [(id, item_name, items[item_name].getpayload(), items[item_name].gettime(), items[item_name].getwho()) for item_name in items]
		for update in updates:
			c.execute('replace in item (node_id, name, payload, time, who) values (?,?,?,?,?)', update)
		updates = [(id, sub.getjid(), sub.getconfig(), sub.getid()) for sub in subscriptions]
		for update in updates:
			c.execute('replace in subscription (node_id, jid, config, subid) values (?,?,?,?)', update)
		self.conn.commit()
		c.close()
	
	def addSubscription(self, node, jid, subid, config=None):
		if config is not None:
			config = ET.tostring(config.getXML())
		c = self.conn.cursor()
		c.execute('select id from node where name=?', (node,))
		id = c.fetchone()[0]
		c.execute('insert into subscription (node_id, jid, config, subid) values (?,?,?,?)', (id, jid, config, subid))
		self.conn.commit()
		c.close()
	
	def updateNodeConfig(self, node, config):
		self.synch(node, config=config)
	
	def getSubscriptions(self, node):
		c = self.conn.cursor()
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
		c = self.conn.cursor()
		c.execute('select config from node where name=?', (node,))
		r = c.fetchone()
		return r[0]
	
	def getNodes(self):
		c = self.conn.cursor()
		c.execute('select name from node')
		return [row[0] for row in c]
	
	def eachNode(self):
		c = self.conn.cursor()
		c.execute('select name, type from node')
		for row in c:
			yield row
		c.close()
	
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
		c = self.conn.cursor()
		c.execute('select affiliation.jid, affiliation.type from node left join affiliation on affiliation.node_id=node.id where node.name=?', (node, ))
		for row in c:
			if row[0] is not None:
				aff[row[1]].append(row[0])
		c.close()
		return aff

