import sleekxmpp.componentxmpp
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.handler.callback import Callback
from xml.etree import cElementTree as ET
import uuid

class BaseNode(object):
	nodetype = 'leaf'

	def __init__(self, pubsub, db, name, config):
		self.pubsub = pubsub
		self.xmpp = self.pubsub.xmpp
		self.db = db
		self.name = name
		self.config = config
		self.subscription_form = {}
		self.publish_form = {}
		self.items = {}
		self.synch = True
		self.affiliations = {'owner': [], 'publisher': [], 'member': [], 'outcast': [], 'subscriber': [], 'pending': []}
		self.dbLoad()
	
	def dbLoad(self):
		self.affiliations = self.db.getAffiliations(self.name)
		self.items = self.db.getItems(self.name)
	
	def dbDump(self):
		pass
	
	def discoverItems(self):
		pass
	
	def getSubscriptions(self):
		pass

	def getAffiliations(self):
		pass
	
	def subscribe(self, config=None):
		pass

	def unsubscribe(self):
		pass
	
	def getSubscriptionOptions(self):
		pass

	def setSubscriptionOptions(self):
		pass
	
	def getItems(self, max=0):
		pass
	
	def publish(self, item, item_id=None, options=None):
		if item_id is None:
			item_id = uuid.uuid4().hex
		payload = item.getchildren()[0]
		for subid, jid in self.affiliations['subscriber']:
			self.notifyItem(payload, jid, item_id)
		return item_id # item id

	def deleteItem(self, id):
		pass
	
	def create(self, config=None):
		pass
	
	def getConfig(self, default=False):
		pass

	def configure(self, config):
		self.config = self.config.merge(config)
		synch = False
	
	def delete(self):
		pass
	
	def purgeNodeItems(self):
		pass
	
	def approvePendingSubscription(self):
		pass

	def modifySubscriptions(self, jids={}):
		pass
	
	def modifyAffiliations(self, jids={}):
		pass
	
	def notifyItem(self, payload, jid, item_id):
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		event = ET.Element('{http://jabber.org/protocol/pubsub#event}event')
		items = ET.Element('items', {'node': self.name})
		item = ET.Element('item', {'id': item_id})
		item.append(payload)
		items.append(item)
		event.append(items)
		msg.append(event)
		self.xmpp.send(msg)
	
	def notifyConfig(self):
		pass
	
	def notifyDeleteItem(self):
		pass
