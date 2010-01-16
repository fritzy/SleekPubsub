import sleekxmpp.componentxmpp
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.handler.callback import Callback
from xml.etree import cElementTree as ET
import uuid
import logging
import pickle
import time

class Subscription(object):
	def __init__(self, node, jid=None, subid=None, config=None, to=None):
		self.node = node
		self.jid = jid
		self.subid = subid
		self.config = config
		self.to = to
	
	def set(self, jid, subid, config, to=None):
		self.jid = jid
		self.subid = subid
		self.config = config
		self.to = to or self.to
	
	def getto(self):
		return self.to
	
	def getjid(self):
		return self.jid
	
	def getid(self):
		return self.subid

	def config(self):
		return config

class Event(object):
	def __init__(self, node):
		self.nodes = [node]
		self.jids = []
	
	def addJid(self, jid):
		self.jids.append(jid)
	
	def hasJid(self, jid):
		return jid in self.jids
	
	def addNode(self, node):
		self.nodes.append(node)
	
	def hasNode(self, node):
		return node in self.nodes
	
	def cleanup(self):
		self.jids = []

class ItemEvent(Event):
	def __init__(self, node, item):
		Event.__init__(self, node)
		self.item = item
	
	def getItem(self):
		return item

class DeleteEvent(ItemEvent):
	pass

class ConfigEvent(Event):
	pass

class Item(object):
	def __init__(self, node, name, who, payload=None, config=None):
		self.node = node
		self.name = name
		self.who = who
		self.payload = payload
		self.config = config
		self.time = time.time()
	
	def getpayload(self):
		return self.payload
	
	def gettime(self):
		return self.time
	
	def getwho(self):
		return self.who


class BaseNode(object):
	nodetype = 'leaf'
	affiliationtypes = ('owner', 'publisher', 'member', 'outcast', 'pending')

	def __init__(self, pubsub, db, name, config=None, owner=None, fresh=False):
		self.new_owner = owner
		self.fresh = fresh
		self.pubsub = pubsub
		self.xmpp = self.pubsub.xmpp
		self.db = db
		self.name = name
		self.config = config
		self.subscription_form = {}
		self.publish_form = {}
		self.items = {}
		self.itemorder = []
		self.synch = True
		self.item_class = Item
		self.affiliations = {'owner': [], 'publisher': [], 'member': [], 'outcast': [], 'pending': []}
		self.subscriptions = {}
		self.subscriptionsbyjid = {}
		if self.new_owner is not None:
			self.affiliations['owner'].append(self.new_owner)
		self.dbLoad()
	
	def dbLoad(self):
		if not self.fresh:
			self.affiliations = self.db.getAffiliations(self.name)
			self.items = self.db.getItems(self.name)
			self.config = pickle.loads(self.db.getNodeConfig(self.name))
			subs = self.db.getSubscriptions(self.name)
			for jid, subid, config in subs:
				self.subscriptions[subid] = Subscription(self, jid, subid, config)
				self.subscriptionsbyjid[jid] = self.subscriptions[subid]
		else:
			self.db.createNode(self.name, self.config, self.affiliations, self.items)

	def dbDump(self):
		self.db.synch(self.name, pickle.dumps(self.config), self.affiliations, self.items)
	
	def discoverItems(self):
		pass
	
	def getSubscriptions(self):
		return self.subscriptions

	def getAffiliations(self):
		return self.affiliations
	
	def subscribe(self, jid, who=None, config=None, to=None):
		subid = uuid.uuid4().hex
		if config is not None:
			config = ET.tostring(config.getXML('submit'))
		self.subscriptions[subid] = Subscription(self, jid, subid, config, to)
		self.subscriptionsbyjid[jid] = self.subscriptions[subid]
		self.db.addSubscription(self.name, jid, subid, config, to)
		return subid
		#TODO modify affiliation

	def unsubscribe(self, jid, who=None, subid=None):
		if subid is None:
			subid = self.subscriptionsbyjid[jid].getsubid()
		self.db.deleteSubscription(self.name, jid, subid)
		try:
			del self.subscriptions[subid]
			if self.subscriptionsbyjid[jid].getsubid() == subid:
				del self.subscriptionsbyjid[jid]
		except IndexError():
			return False
		#TODO add error cases
		#TODO add ACL
		return True
	
	def getSubscriptionOptions(self):
		pass

	def setSubscriptionOptions(self):
		pass
	
	def getItems(self, max=0):
		pass
	
	def getLastItem(self, node):
		pass
	
	def eachSubscriber(self):
		"Generator for subscribers."
		for subid in self.subscriptions:
			subscriber = self.subscriptions[subid]
			jid = subscriber.getjid()
			to = subscriber.getto()
			logging.debug("%s: %s %s" % (jid, '/' in jid, self.config.get('pubsub#presence_based_delivery', False)))
			if '/' in jid or not self.config.get('pubsub#presence_based_delivery', False):
					yield jid, to
			else:
				for resource in self.xmpp.roster.get(jid, {'presence': []})['presence']:
					yield "%s/%s" % (jid, resource)
	
	def publish(self, item, item_id=None, options=None, who=None):
		if item_id is None:
			item_id = uuid.uuid4().hex
		if item.tag == '{http://jabber.org/protocol/pubsub}item':
			payload = item.getchildren()[0]
		else:
			payload = item
		item_inst = self.item_class(self, item_id, who, payload, options)
		if self.config.get('pubsub#persist_items', False):
			self.db.setItem(self.name, item_id, payload)
			self.items[item_id] = item_inst
			if item_id not in self.itemorder:
				self.itemorder.append(item_id)
		event = ItemEvent(self, item_inst)
		self.notifyItem(event)
		max_items = int(self.config.get('pubsub#max_items', 0))
		if max_items != 0 and len(self.itemorder) > max_items:
			self.deleteItem(self.itemorder[0])
		return item_id # item id

	def deleteItem(self, id):
		if id in self.items:
			item = self.items[id]
			del self.items[id]
			self.itemorder.pop(self.itemorder.index(id))
			self.notifyDelete(ItemEvent(self, item))
	
	def create(self, config=None):
		pass
	
	def getConfig(self, default=False):
		return self.config

	def configure(self, config):
		self.config.update(config)
		self.db.synch(self.name, config=pickle.dumps(self.config))
	
	def delete(self):
		pass
	
	def purgeNodeItems(self):
		pass
	
	def approvePendingSubscription(self, jid):
		pass

	def modifySubscriptions(self, jids={}):
		pass
	
	def modifyAffiliations(self, affiliations={}):
		for key in affiliations:
			if key not in self.affiliationtypes:
				return False
		self.affilaitions.update(affiliations)
		self.db.synch(self.name, affiliations=self.affiliations)

	
	def notifyItem(self, event):
		item_id = event.item.name
		payload = event.item.payload
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = ET.Element('{http://jabber.org/protocol/pubsub#event}event')
		items = ET.Element('items', {'node': self.name})
		item = ET.Element('item', {'id': item_id})
		item.append(payload)
		items.append(item)
		xevent.append(items)
		if payload.tag == '{jabber:client}body':
			msg['body'] = payload.text
			msg['type'] = 'chat'
		else:
			msg.append(xevent)
		for jid, to in self.eachSubscriber(): 
			if not event.hasJid(jid):
				event.addJid(jid)
				msg.attrib['to'] = jid
				print("Message is from", to)
				msg['from'] = to or self.xmpp.jid
				self.xmpp.send(msg)
	
	def notifyConfig(self):
		pass
	
	def notifyDelete(self, event):
		pass
