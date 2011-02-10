import sleekxmpp.componentxmpp
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.matcher.stanzapath import StanzaPath
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.plugins import stanza_pubsub as Pubsub
from sleekxmpp.exceptions import XMPPError
from xml.etree import cElementTree as ET
import uuid
from . db import PubsubDB
from . node import BaseNode, CollectionNode, QueueNode, JobNode2
import logging
from . adhoc import PubsubAdhoc
from . httpd import HTTPD
import copy
import os

class NodeCache(object):
	"""Manages nodes in memory, keeping most recently accessed in memory"""
	def __init__(self, pubsub, limit=100, clearbatch=10):
		self.clearbatch = clearbatch
		self.pubsub = pubsub
		self.activenodes = {}
		self.cache = []
		self.limit = limit
		self.allnodes = {}
	
	def __getitem__(self, key):
		if key not in self.allnodes:
			raise KeyError
		if key not in self.activenodes:
			self.loadNode(key)
		self.cache.append(self.cache.pop(self.cache.index(key))) # put node at the end of the cache
		return self.activenodes[key]

	def __contains__(self, key):
		#logging.debug("searching %s for %s" % (self.allnodes, key))
		return key in self.allnodes

	def get(self, key, default=None):
		if key in self:
			return self.__getitem__(key)
		else:
			return default

	def loadNode(self, name):
		if name in self.activenodes:
			return True
		if name not in self.allnodes:
			raise KeyError
		self.activenodes[name] = self.pubsub.node_classes.get(self.allnodes[name], BaseNode)(self.pubsub, self.pubsub.db, name)
		self.cache.append(name)
		self.clearExtra()
	
	def saveAll(self):
		if self.pubsub.config['settings']['node_creation'] != 'createonsubscribe':
			for node in self.allnodes:
				self[node].save()
	
	def addNode(self, name, klass, node=None):
		print "Loading %s as %s" % (node, klass)
		self.allnodes[name] = klass
		if isinstance(node, BaseNode):
			self.cache.append(name)
			self.activenodes[name] = node
			self.clearExtra()
	
	def deleteNode(self, name):
			if name in self.cache:
				del self.activenodes[name]
				self.cache.pop(self.cache.index(name))
			if name in self.allnodes:
				del self.allnodes[name]
	
	def clearExtra(self):
		while len(self.cache) > self.limit:
			for node in self.cache[:self.clearbatch]:
				if node.use_db:
					self.clear(node)
	
	def clearAll(self):
		for node in self.cache:
			self.clear(node)
	
	def clear(self, node):
		if self.pubsub.config['settings']['node_creation'] != 'createonsubscribe':
			self.activenodes[node].save()
		del self.activenodes[node]
		self.cache.pop(self.cache.index(node))
	

class PublishSubscribe(object):
	
	def __init__(self, xmpp, dbfile, settings, rest, overridedefault=None):
		
		self.xmpp = xmpp
		self.dbfile = dbfile
		self.db = None
		self.nodeplugins = []
		
		self.config = {'settings': settings, 'rest': rest}
		if overridedefault is None:
			self.config['defaultnodeconfig'] = {}
		else:
			self.config['defaultnodeconfig'] = overridedefault
		self.default_config = self.getDefaultConfig()
		
		self.admins = []
		self.node_classes = {'leaf': BaseNode, 'collection': CollectionNode, 'queue': QueueNode, 'job': JobNode2}
		self.nodes = NodeCache(self)
		self.adhoc = PubsubAdhoc(self)
		if self.config['rest']['enabled']:
			self.http = HTTPD(self)

		self.presence_expire = {}

		#self.xmpp.registerHandler(Callback('pubsub publish', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><publish xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handlePublish)) 
		self.xmpp.registerHandler(Callback('pubsub state', StanzaPath('iq@type=set/psstate'), self.handleSetState))
		self.xmpp.registerHandler(Callback('pubsub publish', StanzaPath("iq@type=set/pubsub/publish"), self.handlePublish)) 
		self.xmpp.registerHandler(Callback('pubsub create', StanzaPath("iq@type=set/pubsub/create"), self.handleCreateNode)) 
		self.xmpp.registerHandler(Callback('pubsub configure', StanzaPath("iq@type=set/pubsub_owner/configure"), self.handleConfigureNode))
		self.xmpp.registerHandler(Callback('pubsub delete', StanzaPath('iq@type=set/pubsub_owner/delete'), self.handleDeleteNode))
		self.xmpp.registerHandler(Callback('pubsub getitems', StanzaPath('iq@type=get/pubsub/items'), self.handleGetItems))
		self.xmpp.registerHandler(Callback('pubsub delete item', StanzaPath('iq@type=set/pubsub/retract'), self.handleRetractItem))

		#self.xmpp.registerHandler(Callback('pubsub configure', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub#owner'><configure xmlns='http://jabber.org/protocol/pubsub#owner' /></pubsub></iq>" % self.xmpp.default_ns), self.handleConfigureNode)) 
		self.xmpp.registerHandler(Callback('pubsub get configure', MatchXMLMask("<iq xmlns='%s' type='get'><pubsub xmlns='http://jabber.org/protocol/pubsub#owner'><configure xmlns='http://jabber.org/protocol/pubsub#owner' /></pubsub></iq>" % self.xmpp.default_ns), self.handleGetNodeConfig)) 
		self.xmpp.registerHandler(Callback('pubsub defaultconfig', MatchXMLMask("<iq xmlns='%s' type='get'><pubsub xmlns='http://jabber.org/protocol/pubsub#owner'><default xmlns='http://jabber.org/protocol/pubsub#owner' /></pubsub></iq>" % self.xmpp.default_ns), self.handleGetDefaultConfig)) 
		self.xmpp.registerHandler(Callback('pubsub subscribe', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><subscribe xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handleSubscribe)) 
		self.xmpp.registerHandler(Callback('pubsub unsubscribe', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><unsubscribe xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handleUnsubscribe)) 
		self.xmpp.add_event_handler("session_start", self.start)
		self.xmpp.add_event_handler("changed_subscription", self.handlePresenceSubscribe)
		self.xmpp.add_event_handler("got_online", self.handleGotOnline)
		self.xmpp.add_event_handler("got_offline", self.handleGotOffline)
	
	def start(self, event):
		if self.dbfile is not None:
			self.db = PubsubDB(self.dbfile, self.xmpp)
		self.loadNodes()
		self.createNode('__stats__', config={'pubsub#presence_based_delivery': True, 'pubsub#expire': 'presence'}, who=None, use_db=False)
		self.createNode('__purgatory__', config={'pubsub#presence_based_delivery': True, 'pubsub#expire': 'presence'}, who=None, use_db=False)

		self.xmpp.schedule('sleekpubsub_generate_stats', 3.0, self.generateStats, repeat=True)
		if self.db is not None:
			for jid, pfrom in self.db.getRoster():
				if not pfrom: pfrom = self.xmpp.jid
				self.xmpp.sendPresence(pto=jid, ptype='probe', pfrom=pfrom)
				self.xmpp.sendPresence(pto=jid, pfrom=pfrom)
		self.xmpp.event("sleekpubsub_ready", {})

	def save(self):
		self.nodes.saveAll()

	def generateStats(self):
		uptime = os.popen('uptime')
		load = " ".join(uptime.read().strip().split(' ')[-3:])
		uptime.close()
		stats = ET.Element('{http://andyet.net/protocol/jobs}stats')
		loadx = ET.Element('{http://andyet.net/protocol/jobs}load')
		loadx.text = load
		stats.append(loadx)
		for node_name in self.nodes.allnodes:
			if not node_name.startswith("__"):
				node = self.nodes[node_name]
				numworkers = 0
				for jid in node.subscriptionsbyjid:
					numworkers += len(self.xmpp.roster.get(jid, {'presence':{}})['presence'])
				stat = ET.Element('{http://andyet.net/protocol/jobs}stat', {'node': node_name, 'update_rate': "%.2f" % node.updates_per_second, 'size': "%d" % len(node.items), 'max_size': "%s" % node.config.get('pubsub#max_items', '0'), 'workers': "%d" % numworkers})
				stats.append(stat)
		self.publish('__stats__', stats)

	def handleGotOnline(self, pres):
		pfrom = pres['to'].user
		if pfrom: pfrom += "@"
		pfrom += self.xmpp.jid
		self.xmpp.sendPresence(pto=pres['from'].bare, pfrom=pfrom)

	def handleGotOffline(self, pres):
		for node in copy.copy(self.presence_expire.get(pres['from'].full, [])):
			r = self.unsubscribeNode(node, pres['from'].full)
			logging.debug("Unsubscribing %s from %s because they went offline: %s" % (pres['from'], node, r))
		if self.presence_expire.has_key(pres['from'].full) and not self.presence_expire[pres['from'].full]:
			del  self.presence_expire[pres['from'].full]
	
	def handlePresenceSubscribe(self, pres):
		if self.db is None: return
		ifrom = pres['from'].bare
		ito = pres['to'].bare
		subto, subfrom = self.db.getRosterJid(ifrom)
		if True: # pres['to'] == self.xmpp.jid:
			if pres['type'] == 'subscribe':
				if not subto:
					self.xmpp.sendPresenceSubscription(pto=ifrom, pfrom=ito, ptype='subscribed')
					self.db.setRosterTo(ifrom, True, ito)
				if not subfrom:
					self.xmpp.sendPresenceSubscription(pto=ifrom, pfrom=ito, ptype='subscribe')
				self.xmpp.sendPresence(pto=ifrom)
			elif pres['type'] == 'unsubscribe':
				self.xmpp.sendPresenceSubscription(pto=ifrom,  pfrom=ito, ptype='unsubscribed')
				self.xmpp.sendPresenceSubscription(pto=ifrom,  pfrom=ito, ptype='unsubscribe')
				self.db.clearRoster(ifrom)
			elif pres['type'] == 'subscribed':
				if not subfrom:
					self.db.setRosterFrom(ifrom, True)
				if not subto:
					self.xmpp.sendPresenceSubscription(pto=ifrom, pfrom=ito, ptype='subscribed')
					self.db.setRosterTo(ifrom, True, ito)

	def getDefaultConfig(self):
		default_config = self.xmpp.plugin['xep_0004'].makeForm()
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
		default_config.addField('pubsub#expire', label='Expire')
		default_config.addField('pubsub#max_items', label='Max # of items to persist', value='10')
		default_config.addField('pubsub#subscribe', 'boolean', label='Whether to allow subscriptions', value=True)
		default_config.addField('pubsub#collection', 'text-multi', label="This node in collections")
		default_config.addField('sleek#saveonchange', 'boolean', label='Save on every change', value=False)
		default_config.addField('sleek#dupesubscriptions', 'boolean', label='Allow dupe subscriptions', value=True)
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
		return default_config
	
	def loadNodes(self):
		if self.config['settings']['node_creation'] != 'createonsubscribe':
			if self.db is not None:
				for node, node_type in self.db.getNodes():
					self.nodes.addNode(node, node_type)
#[node] = self.node_classes.get(node_type, BaseNode)(self, self.db, node)

	def registerNodeType(self, nodemodule):
		self.nodeplugins.append(nodemodule.extension_class(self))
	
	def registerNodeClass(self, nodeclass):
		self.node_classes[nodeclass.nodetype] = nodeclass
		self.default_config.field['pubsub#node_type'].addOption(nodeclass.nodetype, nodeclass.nodetype.title())
	
	def deleteNode(self, node):
		if node in self.nodes:
			self.nodes[node].delete()
			self.nodes.deleteNode(node)
			if self.db is not None: self.db.deleteNode(node)
			return True
		else:
			return False
	
	def handleDeleteNode(self, stanza):
		if self.deleteNode(stanza['pubsub_owner']['delete']['node']):
			stanza.reply()
			stanza['pubsub_owner']['delete']
			stanza.send()
		else:
			stanza.reply()
			stanza['pubsub_owner']['delete']['node'] = 'somenode'
			stanza['type'] = 'error'
			stanza.send()
	
	def modifyAffiliations(self, node, updates={}, who=None):
		if node in self.nodes:
			return self.nodes[node].modifyAffiliations(updates, who=who)
		else:
			return False
	
	def getAffiliations(self, node, who=None):
		if node in self.nodes:
			return self.nodes[node].getAffiliations(who=who)
		else:
			return False
	
	def handleGetItems(self, iq):
		node = self.nodes.get(iq['pubsub']['items']['node'])
		if node is None:
			iq.reply()
			iq['error']['condition'] = 'item-not-found'
			iq.send()
			return
		item_insts = node.getItems(who=iq['from'])
		if len(item_insts) == 0 or item_insts is None:
			iq.reply()
			iq['error']['condition'] = 'item-not-found'
			iq.send()
			return
		iq.reply()
		for item_inst in item_insts:
			item = Pubsub.Item()
			item['payload'] = item_inst.payload
			item['id'] = item_inst.name
			iq['pubsub']['items'].append(item)
		iq['pubsub']['items']['node'] = node.name
		iq['type'] = 'result'
		iq.send()
	

	def retractItem(self, node, id):
		node = self.nodes.get(node)
		if node is None:
			return False
		node.deleteItem(id)
		return True

	def handleRetractItem(self, iq):
		#TODO should call retract item
		node = self.nodes.get(iq['pubsub']['retract']['node'])
		if node is None:
			iq.reply()
			iq['error']['condition'] = 'item-not-found'
			iq.send()
			return
		for item in iq['pubsub']['retract']:
			node.deleteItem(item['id'])
		iq.reply()
		iq.send()


	def handlePublish(self, stanza):
		"""iq/{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}publish"""
		node = self.nodes.get(stanza['pubsub']['publish']['node'])
		ids = []
		if node is None:
			stanza.reply()
			stanza['error']['condition'] = 'item-not-found'
			stanza.send()
			return
		for item in stanza['pubsub']['publish']:
			item_id = self.publish(stanza['pubsub']['publish']['node'], item['payload'], item['id'], stanza['from'].bare)
			ids.append(item_id)
		stanza.reply()
		#stanza['pubsub'].clear()
		for id in ids:
			item = Pubsub.Item()
			item['id'] = id
			stanza['pubsub']['publish'].append(item)
		stanza.send()
	
	def handleSetState(self, iq):
		node = self.nodes.get(iq['psstate']['node'])
		payload = iq['psstate']['payload']
		if node is None:
			raise XMPPError('item-not-found')
		item = iq['psstate']['item']
		if item is not None:
			result = node.setItemState(item, payload, iq['from'])
			if result:
				iq.reply()
				iq['psstate']['payload'] = payload
				iq.send()
			else:
				iq.reply()
				iq['error']['condition'] = 'not-allowed'
				iq.send()
				#raise XMPPError('not-allowed')
	
	def publish(self, node, item, id=None, who=None):
		if isinstance(node, str):
			node = self.nodes.get(node)
		return node.publish(item, id, who=who)
	
	def handleGetDefaultConfig(self, stanza):
		stanza.reply()
		stanza['pubsub_owner']['default']['config'] = self.node_classes.get(stanza['pubsub_owner']['default']['type'], BaseNode).default_config
		stanza.send()
	
	def createNode(self, node, config=None, who=None, use_db=True):
		if config is None:
			config = copy.copy(self.default_config)
			for option in self.config['defaultnodeconfig']:
				config.field[option].setValue(self.config['defaultnodeconfig'][option])
				#config.setValues({option: self.config.get('defaultnodeconfig', option)})
		else:
			config = self.default_config.merge(config)
		config = config.getValues()
		nodeclass = self.node_classes.get(config['pubsub#node_type'])
		if node in self.nodes or nodeclass is None:
			return False
		if who:
			who = self.xmpp.getjidbare(who)
		self.nodes.addNode(node, nodeclass, nodeclass(self, self.db, node, config, owner=who, fresh=True, use_db=use_db))
		return True
	
	def handleCreateNode(self, iq):
		node = iq['pubsub']['create']['node'] or uuid.uuid4().hex
		config = iq['pubsub']['configure']['form'] or self.default_config
		if node in self.nodes:
			raise XMPPError('conflict', etype='cancel')
		if not self.createNode(node, config, iq['from'].full):
			raise XMPPError()
		iq.reply()
		iq['pubsub']['create']['node'] = node
		iq.send()
	
	def configureNode(self, node, config):
		if node not in self.nodes:
			return False
		config = self.default_config.merge(config).getValues()
		self.nodes[node].configure(config)
		return True
	
	def handleConfigureNode(self, stanza):
		xml = stanza.xml
		configure = xml.find('{http://jabber.org/protocol/pubsub#owner}pubsub/{http://jabber.org/protocol/pubsub#owner}configure')
		node = configure.get('node')
		xform = xml.find('{http://jabber.org/protocol/pubsub#owner}pubsub/{http://jabber.org/protocol/pubsub#owner}configure/{jabber:x:data}x')
		if xform is None or not self.configureNode(node, self.xmpp.plugin['xep_0004'].buildForm(xform)):
			self.xmpp.send(self.xmpp.makeIqError(xml.get('id')))
			return
		iq = self.xmpp.makeIqResult(xml.get('id'))
		iq.attrib['from'] = self.xmpp.jid
		iq.attrib['to'] = xml.get('from')
		self.xmpp.send(iq)
	
	def subscribeNode(self, node, jid, who=None, to=None):
		print "subscribe node"
		if node not in self.nodes:
			if self.config['settings']['node_creation'] == 'createonsubscribe':
				self.createNode(node, config=None, who=who.full)
			else:
				print "no create on subscribe"
				return False
		if self.nodes[node].config.get('pubsub#expire') == 'presence':
			print "presence expire"
			if not self.xmpp.roster.has_key(jid.bare) or not self.xmpp.roster[jid.bare]['presence'].has_key(jid.resource):
				print "could not subscribe to pubsub#expire", jid.full, self.xmpp.roster
				return False
			else:
				print "subscribing"
				if str(jid) not in self.presence_expire:
					self.presence_expire[str(jid)] = []
				self.presence_expire[str(jid)].append(node)
				print self.presence_expire
		if who is not None:
			who = who.full
		return self.nodes[node].subscribe(jid.full, who, to=to)
	
	def handleSubscribe(self, stanza):
		node = stanza['pubsub']['subscribe']['node']
		jid = stanza['pubsub']['subscribe']['jid']
		subid = self.subscribeNode(node, jid, stanza['from'])
		if not subid:
			raise XMPPError('not-allowed')
			#self.xmpp.send(self.xmpp.makeIqError(stanza['id']))
			return
		stanza.reply()
		stanza.clear()
		stanza['pubsub']['subscription']['subid'] = subid
		stanza['pubsub']['subscription']['node'] = node
		stanza['pubsub']['subscription']['jid'] = str(jid)
		stanza['pubsub']['subscription']['subscription'] = 'subscribed'
		stanza.send()
	
	def handleUnsubscribe(self, stanza):
		xml = stanza.xml
		subscribe = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}unsubscribe')
		node = subscribe.get('node')
		jid = subscribe.get('jid')
		subid = subscribe.get('subid')
		if node not in self.nodes:
			self.xmpp.send(self.xmpp.makeIqError(xml.get('id')))
			return
		self.nodes[node].unsubscribe(jid, stanza['from'].bare, subid)
		iq = self.xmpp.makeIqResult(xml.get('id'))
		iq.attrib['from'] = self.xmpp.jid
		iq.attrib['to'] = xml.get('from')
		self.xmpp.send(iq)
	
	def unsubscribeNode(self, node, jid, who=None, subid=None):
		if node in self.nodes:
			self.nodes[node].unsubscribe(jid, who, subid)
			if self.nodes[node].config.get('pubsub#expire') == 'presence':
				if jid in self.presence_expire and node in self.presence_expire[jid]:
					self.presence_expire[jid].pop(self.presence_expire[jid].index(node))
			if self.config['settings']['node_creation'] == 'createonsubscribe' and not self.nodes[node].subscriptionsbyjid:
				self.deleteNode(node)
			return True
		else:
			return False
	
	def getNodeConfig(self, node):
		if node not in self.nodes:
			return False
		config = copy.copy(self.default_config)
		config.setValues(self.nodes[node].getConfig())
		return config

	def handleGetNodeConfig(self, stanza):
		xml = stanza.xml
		configure = xml.find('{http://jabber.org/protocol/pubsub#owner}pubsub/{http://jabber.org/protocol/pubsub#owner}configure')
		node = configure.get('node')
		config = self.getNodeConfig(node)
		if config == False:
			self.xmpp.send(self.xmpp.makeIqError(xml.get('id')))
			return
		config = config.getXML('form')
		iq = self.xmpp.makeIqResult(xml.get('id'))
		iq.attrib['from'] = self.xmpp.jid
		iq.attrib['to'] = xml.get('from')
		pubsub = ET.Element('{http://jabber.org/protocol/pubsub#owner}pubsub')
		configure = ET.Element('{http://jabber.org/protocol/pubsub#owner}configure', {'node': node})
		configure.append(config)
		pubsub.append(configure)
		iq.append(pubsub)
		self.xmpp.send(iq)
