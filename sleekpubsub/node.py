import sleekxmpp.componentxmpp
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.plugins.xep_0004 import Form
from xml.etree import cElementTree as ET
import uuid
import logging
import pickle
import time

class StateMachine(object):
    def __init__(self, resource, ns):
        self.ns = ns
        self.resource = resource
        self.registers = {}
        self.statexml = None

    def registerStateCallback(self, current, new, callback):
        self.registers[(current, new)] = callback

    def setState(self, xml):
        current = None
        if self.statexml is not None:
            current = self.statexml.tag
        if (current, xml.tag) not in self.registers:
            return False
        passed = self.registers[(current, xml.tag)]
        if passed:
            self.statexml = xml
            self.resource.saveState(xml)

    def getState(self):
        if self.statexml is None:
            return None
        return self.statexml.tag
    
    def getXML(self):
        return self.statexml

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
		self.nodes = []
		self.jids = []
		self.originalnode = node
	
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
	def __init__(self, name, item):
		Event.__init__(self, name)
		self.item = item
	
	def getItem(self):
		return item

class QueueItemEvent(ItemEvent):
    def __init__(self, name, item, bcast=None, rotation=0):
        ItemEvent__init__(self, name, item)
        self.bcast = bcast
        self.rotation = rotation
        self.idx = 0
		subgen = None
	
	def reset(self):
		self.idx = 0
	
	def setSubGen(self, subgen):
		self.subgen = subgen
	
	def next(self):
		if self.subgen is not None:
			return self.subgen.next()
		else:
			return []
		

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
        self.state = None
	
	def getpayload(self):
		return self.payload
	
	def gettime(self):
		return self.time
	
	def getwho(self):
		return self.who

    def saveState(self, xml):
        self.node._saveItemState(self.name, xml)

class QueueItem(Item):
    def __init__(self, *args, **kwargs):
        super(QueueItem, self).__init__()
        self.state['http://andyet.net/protocol/pubsubqueue'] = StateMachine(self)

class BaseNode(object):
	nodetype = 'leaf'
	affiliationtypes = ('owner', 'publisher', 'member', 'outcast', 'pending')

	default_config = Form(None, title='Leaf Config Form')
	default_config.addField('FORM_TYPE', 'hidden', value='http://jabber.org/protocol/pubsub#node_config')
	ntype = default_config.addField('pubsub#node_type', 'list-single', label='Select the node type', value='leaf')
	ntype.addOption('leaf', 'Leaf')
	default_config.addField('pubsub#title', label='A friendly name for the node')
	default_config.addField('pubsub#deliver_notifications', 'boolean', label='Deliver event notifications', value=True)
	default_config.addField('pubsub#deliver_payloads', 'boolean', label='Deliver payloads with event notifications', value=True)
	default_config.addField('pubsub#notify_config', 'boolean', label='Notify subscribers when the node configuration changes', value=False)
	default_config.addField('pubsub#notify_delete', 'boolean', label='Notify subscribers when the node is deleted', value=False)
	default_config.addField('pubsub#notify_retract', 'boolean', label='Notify subscribers when items are removed from the node', value=False)
	default_config.addField('pubsub#notify_sub', 'boolean', label='Notify owners about new subscribers and unsubscribes', value=False)
	default_config.addField('pubsub#persist_items', 'boolean', label='Persist items in storage', value=False)
	default_config.addField('pubsub#max_items', label='Max # of items to persist', value='10')
	default_config.addField('pubsub#subscribe', 'boolean', label='Whether to allow subscriptions', value=True)
	default_config.addField('pubsub#collection', 'text-multi', label="This node in collections")
	default_config.addField('sleek#saveonchange', 'boolean', label='Save on every change', value=False)
	model = default_config.addField('pubsub#access_model', 'list-single', label='Specify the subscriber model', value='open')
	#model.addOption('authorize', 'Authorize') # not yet implemented
	model.addOption('open', 'Open')
	model.addOption('whitelist', 'whitelist')
	model = default_config.addField('pubsub#publish_model', 'list-single', label='Specify the publisher model', value='publishers')
	model.addOption('publishers', 'Publishers')
	model.addOption('subscribers', 'Subscribers')
	model.addOption('open', 'Open')
	model = default_config.addField('pubsub#send_last_published_item', 'list-single', label='Send last published item', value='never')
	model.addOption('never', 'Never')
	model.addOption('on_sub', 'On Subscription')
	model.addOption('on_sun_and_presence', 'On Subscription And Presence')
	default_config.addField('pubsub#presence_based_delivery', 'boolean', label='Deliver notification only to available users', value=False)
	del ntype
	del model

    item_class = Item
    itemevent_class = ItemEvent

	def __init__(self, pubsub, db, name, config=None, owner=None, fresh=False):
		self.new_owner = owner
		self.fresh = fresh
		self.pubsub = pubsub
		self.xmpp = self.pubsub.xmpp
		self.db = db
		self.name = name
		self.collections = []
		self.config = config or self.default_config.copy()
		self.subscription_form = {}
		self.publish_form = {}
		self.items = {}
		self.itemorder = []
		self.synch = True
        self.state = ''
		#self.affiliations = {'owner': [], 'publisher': [], 'member': [], 'outcast': [], 'pending': []}
		self.affiliations = {}
		for afftype in self.affiliationtypes:
			self.affiliations[afftype] = []
		self.subscriptions = {}
		self.subscriptionsbyjid = {}
		if self.new_owner is not None:
			self.affiliations['owner'].append(self.new_owner)
		self.dbLoad()
		self.lastsaved = time.time()
	
	def dbLoad(self):
		if not self.fresh:
			self.affiliations = self.db.getAffiliations(self.name)
			self.items = self.db.getItems(self.name)
			self.config = pickle.loads(self.db.getNodeConfig(self.name))
			parentset = self._checkconfigcollections(self.config, False)
			if not parentset: logging.warning("Was not able to set all parents in %s" % self.name)
			subs = self.db.getSubscriptions(self.name)
			for jid, subid, config in subs:
				self.subscriptions[subid] = Subscription(self, jid, subid, config)
				self.subscriptionsbyjid[jid] = self.subscriptions[subid]
		else:
			self.db.createNode(self.name, self.config, self.affiliations, self.items)

	def dbDump(self):
		self.db.synch(self.name, pickle.dumps(self.config), self.affiliations, self.items)
		self.lastsaved = time.time()

	def save(self):
		self.dbDump()

	def discoverItems(self):
		pass
	
	def getSubscriptions(self):
		return self.subscriptions

	def getAffiliations(self):
		return self.affiliations
	
	def subscribe(self, jid, who=None, config=None, to=None):
		#print(who, self.affiliations['owner'])
		if (
			(who is None or who in self.affiliations['owner'] or who.startswith(jid)) and 
			(self.config['pubsub#access_model'] == 'open' or 
				(self.config['pubsub#access_model'] == 'whitelist' and jid in self.affiliations['member']) or
				(who in self.affiliations['owner'])
			)
		):
			subid = uuid.uuid4().hex
			if config is not None:
				config = ET.tostring(config.getXML('submit'))
			self.subscriptions[subid] = Subscription(self, jid, subid, config, to)
			self.subscriptionsbyjid[jid] = self.subscriptions[subid]
			if self.config['sleek#saveonchange']:
				self.db.addSubscription(self.name, jid, subid, config, to)
			return subid
		else:
			return False
		#TODO modify affiliation

	def unsubscribe(self, jid, who=None, subid=None):
		if subid is None:
			subid = self.subscriptionsbyjid[jid].getid()
		if self.config['sleek#saveonchange']:
			self.db.deleteSubscription(self.name, jid, subid)
		try:
			del self.subscriptions[subid]
			if self.subscriptionsbyjid[jid].getid() == subid:
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
	
	def eachSubscriber(self, step=1):
		"Generator for subscribers."
		if step < 1:
			raise ValueError
		subscriptions = self.subscriptions.keys()
		result = []
		idx = 0 # would enumerate, but num of results isn't necessary the same as len(subscriptions)
		for subid in subscriptions:
			subscriber = self.subscriptions[subid]
			jid = subscriber.getjid()
			to = subscriber.getto()
			logging.debug("%s: %s %s" % (jid, '/' in jid, self.config.get('pubsub#presence_based_delivery', False)))
			if '/' in jid or not self.config.get('pubsub#presence_based_delivery', False):
					result.append((jid, to))
					if idx % step == 0:
						yield result
						result = []
					idx += 1
			else:
				for resource in self.xmpp.roster.get(jid, {'presence': []})['presence']:
					result.append(("%s/%s" % (jid, resource), to))
					if idx % step == 0:
						yield result
						result = []
					idx += 1
	
	def publish(self, item, item_id=None, options=None, who=None):
		if item_id is None:
			item_id = uuid.uuid4().hex
		if item.tag == '{http://jabber.org/protocol/pubsub}item':
			payload = item.getchildren()[0]
		else:
			payload = item
		item_inst = self.item_class(self, item_id, who, payload, options)
		if self.config.get('pubsub#persist_items', False):
			if self.config['sleek#saveonchange']:
				self.db.setItem(self.name, item_id, payload)
			self.items[item_id] = item_inst
			if item_id not in self.itemorder:
				self.itemorder.append(item_id)
			else:
				self.itemorder.append(self.itemorder.pop(self.itemorder.index(item_id)))
		event = self.itemevent_class(self.name, item_inst)
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
			self.notifyDelete(ItemEvent(self, self.name, item))
	
	def _checkconfigcollections(self, config, reconfigure=True):
		collections = []
		passed = True
		nodes = config.get('pubsub#collection', [])
		if type(nodes) != type([]):
			nodes = [nodes]
		for node in nodes:
			if node not in self.pubsub.nodes:
				passed = False
			else:
				collections.append(node)
		if not reconfigure or passed:
			self.collections = collections
		return passed
	
	def create(self, config=None):
		pass
	
	def getConfig(self, default=False):
		return self.config

	def configure(self, config):
		if not self._checkconfigcollections(config):
			raise XMPPError() #TODO make this the right error
		self.config.update(config)
		# we do this regardless of cache settings
		self.db.synch(self.name, config=pickle.dumps(self.config))
    
    def setState(self, state):
        pass

    def setItemState(self, item_id, state):
        pass
	
    def _saveState(self, xml):
        pass

    def _saveItemState(self, node, xml):
        pass

	def deleteItem(self, item_id):
		pass
	
	def purgeNodeItems(self):
		pass
	
	def approvePendingSubscription(self, jid):
		pass

	def modifySubscriptions(self, jids={}):
		pass
	
	def modifyAffiliations(self, affiliations={}, who=None):
		if who is not None and who not in self.affiliations['owner']:
			return False
		for key in affiliations:
			if key not in self.affiliationtypes:
				return False
		self.affiliations.update(affiliations)
		if self.config['sleek#saveonchange']:
			self.db.synch(self.name, affiliations=self.affiliations)
		return True
	
	def getAffiliations(self, who=None):
		if who is not None and who not in self.affiliations['owner']:
			return False
		return self.affiliations
	
	def notifyItem(self, event):
		if event.hasNode(self.name):
			return False
		event.addNode(self.name)
		item_id = event.item.name
		payload = event.item.payload
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = ET.Element('{http://jabber.org/protocol/pubsub#event}event')
		items = ET.Element('items', {'node': event.originalnode})
		item = ET.Element('item', {'id': item_id})
		item.append(payload)
		items.append(item)
		xevent.append(items)
		if payload.tag == '{jabber:client}body':
			msg['body'] = payload.text
			msg['type'] = 'chat'
		else:
			msg.append(xevent)
		for toset in self.eachSubscriber(): 
			jid, mto = toset[0]
			if not event.hasJid(jid):
				event.addJid(jid)
				msg.attrib['to'] = jid
				#print("Message is from", mto)
				msg['from'] = mto or self.xmpp.jid
				self.xmpp.send(msg)
		for parent in self.collections:
			if parent in self.pubsub.nodes:
				self.pubsub.nodes[parent].notifyItem(event)
	
	def notifyConfig(self):
		pass
	
	def notifyDelete(self, event):
		pass

	def delete(self):
		for sub in self.subscriptions.keys():
			del self.subscriptions[sub]
		for sub in self.subscriptionsbyjid.keys():
			del self.subscriptionsbyjid[sub]
		for item in self.items.keys():
			del self.items[item]
		for collection in self.collections:
			self.collections.pop(self.collections.index(collection))

class CollectionNode(BaseNode):

	def publish(self, *args, **kwargs):
		return False

	def deleteItem(self, *args, **kwargs):
		return False

class QueueNode(BaseNode):
	default_config = BaseNode.default_config.copy()
	bcast = default_config.addField("queue#braodcast", "list-single", label="Broadcast behavior", value="broadcast")
	bcast.addOption('broadcast', 'Broadcast')
	bcast.addOption('roundrobin', "Round Robin")
	bcast.addOption('hybrid', 'Hybrid')
	del bcast
	default_config.addField('queue#bcaststep', label='Number of subscribers to step to in round robin', value='4')
	default_config.addField('queue#wait', label='Seconds to wait for subscribers to claim', value='4')
    item_class = QueueItem
    itemevent_class = QueueItemEvent

    def __init__(self, *args, **kwargs):
        BaseNode.__init__(self, *args, **kwargs)
        self.current_event = None

    def setState(self, state):
        super(QueueNode, self).setState(state)

    def setItemState(self, item_id, state):
        super(QueueNode, self).setState(state)
	
    def notifyItem(self, event):
		if event.hasNode(self.name) or self.current_event is None:
            return
		event.addNode(self.name)
		subgen = self.eachSubscriber(step=int(self.config.get('queue#bcaststep', 1)))
        event.setSubGen(subgen)
        self._broadcast()
		for parent in self.collections:
			if parent in self.pubsub.nodes:
				self.pubsub.nodes[parent].notifyItem(event)
        
    def _broadcast(self):
        if self.current_event is None:
            return
        event = self.current_event
		item_id = event.item.name
		payload = event.item.payload
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = ET.Element('{http://jabber.org/protocol/pubsub#event}event')
		items = ET.Element('items', {'node': event.originalnode})
		item = ET.Element('item', {'id': item_id})
		item.append(payload)
		items.append(item)
		xevent.append(items)
		if payload.tag == '{jabber:client}body':
			msg['body'] = payload.text
			msg['type'] = 'chat'
		else:
			msg.append(xevent)
        try:
            for jid, mto in event.next():
                if not event.hasJid(jid):
                    event.addJid(jid)
                    msg.attrib['to'] = jid
                    #print("Message is from", mto)
                    msg['from'] = mto or self.xmpp.jid
                    self.xmpp.send(msg)
            self.xmpp.schedule("%s::%s::bcast" % (self.name, item_id), float(self.config.get('queue#wait', 3)), self._broadcast, tuple())
        except StopIteration:
            #start broadcasting again
            self.current_event = None
            self.notifyItem(event)
