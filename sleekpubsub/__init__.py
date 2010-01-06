import sleekxmpp.componentxmpp
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.handler.callback import Callback
from xml.etree import cElementTree as ET
import uuid
from . db import PubsubDB
from . node import BaseNode
import logging
from . adhoc import PubsubAdhoc

class PublishSubscribe(object):
	
	def __init__(self, xmpp, dbfile):
		self.xmpp = xmpp
		self.dbfile = dbfile
		self.adhoc = PubsubAdhoc(self)
		self.nodeplugins = []
		
		self.default_config = self.getDefaultConfig()
		self.nodeset = set()
		
		self.admins = []
		self.node_classes = {'leaf': BaseNode}
		self.nodes = {}

		self.xmpp.registerHandler(Callback('pubsub publish', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><publish xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handlePublish)) 
		self.xmpp.registerHandler(Callback('pubsub create', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><create xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handleCreateNode)) 
		self.xmpp.registerHandler(Callback('pubsub configure', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub#owner'><configure xmlns='http://jabber.org/protocol/pubsub#owner' /></pubsub></iq>" % self.xmpp.default_ns), self.handleConfigureNode)) 
		self.xmpp.registerHandler(Callback('pubsub get configure', MatchXMLMask("<iq xmlns='%s' type='get'><pubsub xmlns='http://jabber.org/protocol/pubsub#owner'><configure xmlns='http://jabber.org/protocol/pubsub#owner' /></pubsub></iq>" % self.xmpp.default_ns), self.handleGetNodeConfig)) 
		self.xmpp.registerHandler(Callback('pubsub defaultconfig', MatchXMLMask("<iq xmlns='%s' type='get'><pubsub xmlns='http://jabber.org/protocol/pubsub#owner'><default xmlns='http://jabber.org/protocol/pubsub#owner' /></pubsub></iq>" % self.xmpp.default_ns), self.handleGetDefaultConfig)) 
		self.xmpp.registerHandler(Callback('pubsub subscribe', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><subscribe xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handleSubscribe)) 
		self.xmpp.registerHandler(Callback('pubsub unsubscribe', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><unsubscribe xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handleUnsubscribe)) 
		self.xmpp.add_event_handler("session_start", self.start)
		self.xmpp.add_event_handler("changed_subscription", self.handlePresenceSubscribe)
	
	def start(self, event):
		self.db = PubsubDB(self.dbfile, self.xmpp)
		self.loadNodes()
		for jid in self.db.getRoster():
			self.xmpp.sendPresence(pto=jid, ptype='probe')
			self.xmpp.sendPresence(pto=jid)

	def handlePresenceSubscribe(self, event):
		ifrom = self.xmpp.getjidbare(event['from'])
		ito = self.xmpp.getjidbare(event['to'])
		print("Handling...")
		subto, subfrom = self.db.getRosterJid(ifrom)
		print((subto, subfrom))
		if event['to'] == self.xmpp.jid:
			if event['type'] == 'subscribe':
				if not subto:
					print("sending out subscribed")
					self.xmpp.sendPresenceSubscription(pto=ifrom, pfrom=ito, ptype='subscribed')
					self.db.setRosterTo(ifrom, True)
				if not subfrom:
					self.xmpp.sendPresenceSubscription(pto=ifrom, pfrom=ito, ptype='subscribe')
				self.xmpp.sendPresence(pto=ifrom)
			elif event['type'] == 'unsubscribe':
				self.xmpp.sendPresenceSubscription(pto=ifrom,  pfrom=ito, ptype='unsubscribed')
				self.xmpp.sendPresenceSubscription(pto=ifrom,  pfrom=ito, ptype='unsubscribe')
				self.db.clearRoster(ifrom)
			elif event['type'] == 'subscribed':
				if not subfrom:
					self.db.setRosterFrom(ifrom, True)
				if not subto:
					self.xmpp.sendPresenceSubscription(pto=ifrom, pfrom=ito, ptype='subscribed')
					self.db.setRosterTo(ifrom, True)

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
		default_config.addField('pubsub#max_items', label='Max # of items to persist', value='10')
		default_config.addField('pubsub#subscribe', 'boolean', label='Whether to allow subscriptions', value=True)
		model = default_config.addField('pubsub#access_model', 'list-single', label='Specify the subscriber model', value='open')
		model.addOption('authorize', 'Authorize')
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
		for node, node_type in self.db.getNodes():
			self.nodes[node] = self.node_classes.get(node_type, BaseNode)(self, self.db, node)
			self.nodeset.update((node,))

	def registerNodeType(self, nodemodule):
		self.nodeplugins.append(nodemodule.extension_class(self))
	
	def registerNodeClass(self, nodeclass):
		self.node_classes[nodeclass.nodetype] = nodeclass
		self.default_config.field['pubsub#node_type'].addOption(nodeclass.nodetype, nodeclass.nodetype.title())
	
	def handlePublish(self, stanza):
		"""iq/{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}publish"""
		xml = stanza.xml
		id = xml.get('id')
		publish = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}publish')
		logging.debug("Publish xml %s" % publish)
		node = publish.get('node')
		nodei = self.nodes.get(node)
		items = publish.findall('{http://jabber.org/protocol/pubsub}item')
		logging.debug("Items: %s" % items)
		ids = []
		if nodei is None:
			"TODO: Error here."
			logging.error("Node not found.")
			iq = self.xmpp.makeIqError(id)
		else:
			for item in items:
				logging.debug(item)
				item_id = item.get('id')
				item_id = nodei.publish(item, item_id, who=xml.get('from'))
				ids.append(item_id)
			iq = self.xmpp.makeIqResult(id)
		iq.attrib['to'] = xml.get('from')
		iq.attrib['from'] = self.xmpp.jid
		pubsub = ET.Element('{http://jabber.org/protocol/pubsub}pubsub')
		publish = ET.Element('publish', {'node': node})
		for item_id in ids:
			nitem = ET.Element('item', {'id': item_id})
			publish.append(nitem)
		pubsub.append(publish)
		iq.append(pubsub)
		self.xmpp.send(iq)
	
	def publish(self, node, item, id, who=None):
		if isinstance(node, str):
			node = self.nodes.get(node)
		return node.publish(item, id, who=who)
	
	def handleGetDefaultConfig(self, stanza):
		xml = stanza.xml
		xml.attrib['to'] = xml.attrib['from']
		xml.attrib['from'] = self.xmpp.jid
		xml.attrib['type'] = 'result'
		default = xml.find('{http://jabber.org/protocol/pubsub#owner}pubsub/{http://jabber.org/protocol/pubsub#owner}default')
		default.append(self.default_config.getXML('form'))
		self.xmpp.send(xml)
	
	def createNode(self, node, config=None, who=None):
		if config is None:
			config = self.default_config.copy()
		else:
			config = self.default_config.merge(config)
		config = config.getValues()
		nodeclass = self.node_classes.get(config['pubsub#node_type'])
		if node in self.nodeset or nodeclass is None:
			return False
		if who:
			who = self.xmpp.getjidbare(who)
		self.nodes[node] = nodeclass(self, self.db, node, config, owner=who, fresh=True)
		self.nodeset.update((node,))
		return True
	
	def handleCreateNode(self, stanza):
		xml = stanza.xml
		create = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}create')
		node = create.get('node', uuid.uuid4().hex)
		xform = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}configure/{jabber:x:data}x')
		if xform is None:
			#logging.warning("No Config included")
			xform = self.default_config.getXML('submit')
		config = self.xmpp.plugin['xep_0004'].buildForm(xform)
		if not self.createNode(node, config. xml.get('from')):
			self.xmpp.send(self.xmpp.makeIqError(xml.get('id')))
			return
		iq = self.xmpp.makeIqResult(xml.get('id'))
		iq.attrib['from'] = self.xmpp.jid
		iq.attrib['to'] = xml.get('from')
		pubsub = ET.Element('{http://jabber.org/protocol/pubsub}pubsub')
		create = ET.Element('{http://jabber.org/protocol/pubsub}create', {'node': node})
		pubsub.append(create)
		iq.append(pubsub)
		self.xmpp.send(iq)
	
	def configureNode(self, node, config):
		if node not in self.nodeset:
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
	
	def subscribeNode(self, node, jid, who=None):
		if node not in self.nodeset:
			return False
		return self.nodes[node].subscribe(jid, who)
	
	def handleSubscribe(self, stanza):
		xml = stanza.xml
		subscribe = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}subscribe')
		node = subscribe.get('node')
		jid = subscribe.get('jid')
		subid = self.subscribeNode(self, node, jid, xml.get('from'))
		if not subid:
			self.xmpp.send(self.xmpp.makeIqError(xml.get('id')))
			return
		iq = self.xmpp.makeIqResult(xml.get('id'))
		iq.attrib['from'] = self.xmpp.jid
		iq.attrib['to'] = xml.get('from')
		#TODO pass back subscription id
		self.xmpp.send(iq)
	
	def handleUnsubscribe(self, stanza):
		xml = stanza.xml
		subscribe = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}unsubscribe')
		node = subscribe.get('node')
		jid = subscribe.get('jid')
		subid = subscribe.get('subid')
		if node not in self.nodeset:
			self.xmpp.send(self.xmpp.makeIqError(xml.get('id')))
			return
		self.nodes[node].unsubscribe(jid, xml.get('from'), subid)
		iq = self.xmpp.makeIqResult(xml.get('id'))
		iq.attrib['from'] = self.xmpp.jid
		iq.attrib['to'] = xml.get('from')
		self.xmpp.send(iq)
	
	def getNodeConfig(self, node):
		if node not in self.nodeset:
			return False
		config = self.default_config.copy()
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
