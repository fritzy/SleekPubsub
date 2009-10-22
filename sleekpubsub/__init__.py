import sleekxmpp.componentxmpp
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.handler.callback import Callback
from xml.etree import cElementTree as ET
import uuid
from . db import PubsubDB
from . node import BaseNode
import logging

class PublishSubscribe(object):
	
	def __init__(self, xmpp):
		self.xmpp = xmpp
		
		self.default_config = self.xmpp.plugin['xep_0004'].makeForm()
		self.default_config.addField('FORM_TYPE', 'hidden', value='http://jabber.org/protocol/pubsub#node_config')
		ntype = self.default_config.addField('pubsub#node_type', 'list-single', label='Select the node type', value='leaf')
		ntype.addOption('leaf', 'Leaf')
		self.default_config.addField('pubsub#title', label='A friendly name for the node')
		self.default_config.addField('pubsub#deliver_notifications', 'boolean', label='Deliver event notifications', value=True)
		self.default_config.addField('pubsub#deliver_payloads', 'boolean', label='Deliver payloads with event notifications', value=True)
		self.default_config.addField('pubsub#notify_config', 'boolean', label='Notify subscribers when the node configuration changes', value=False)
		self.default_config.addField('pubsub#notify_delete', 'boolean', label='Notify subscribers when the node is deleted', value=False)
		self.default_config.addField('pubsub#notify_retract', 'boolean', label='Notify subscribers when items are removed from the node', value=False)
		self.default_config.addField('pubsub#notify_sub', 'boolean', label='Notify owners about new subscribers and unsubscribes', value=False)
		self.default_config.addField('pubsub#persist_items', 'boolean', label='Persist items in storage', value=False)
		self.default_config.addField('pubsub#max_items', label='Max # of items to persist', value='10')
		self.default_config.addField('pubsub#subscribe', 'boolean', label='Whether to allow subscriptions', value=True)
		model = self.default_config.addField('pubsub#access_model', 'list-single', label='Specify the subscriber model', value='open')
		model.addOption('authorize', 'Authorize')
		model.addOption('open', 'Open')
		model.addOption('whitelist', 'whitelist')
		model = self.default_config.addField('pubsub#publish_model', 'list-single', label='Specify the publisher model', value='publishers')
		model.addOption('publishers', 'Publishers')
		model.addOption('subscribers', 'Subscribers')
		model.addOption('open', 'Open')
		model = self.default_config.addField('pubsub#send_last_published_item', 'list-single', label='Send last published item', value='never')
		model.addOption('never', 'Never')
		model.addOption('on_sub', 'On Subscription')
		model.addOption('on_sun_and_presence', 'On Subscription And Presence')
		self.default_config.addField('pubsub#presence_based_delivery', 'boolean', label='Deliver notification only to available users', value=False)
		
		self.admins = []
		self.node_classes = {'leaf': BaseNode}
		self.nodes = {}

		self.xmpp.registerHandler(Callback('pubsub publish', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><publish xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handlePublish)) 
		self.xmpp.registerHandler(Callback('pubsub create', MatchXMLMask("<iq xmlns='%s' type='set'><pubsub xmlns='http://jabber.org/protocol/pubsub'><create xmlns='http://jabber.org/protocol/pubsub' /></pubsub></iq>" % self.xmpp.default_ns), self.handleCreateNode)) 
		self.xmpp.registerHandler(Callback('pubsub defaultconfig', MatchXMLMask("<iq xmlns='%s' type='get'><pubsub xmlns='http://jabber.org/protocol/pubsub#owner'><default xmlns='http://jabber.org/protocol/pubsub#owner' /></pubsub></iq>" % self.xmpp.default_ns), self.handleGetDefaultConfig)) 

		self.db = PubsubDB('pubsub.db')
		self.loadNodes()
	
	def loadNodes(self):
		self.nodes = {'testnode': BaseNode(self, self.db, 'testnode', self.default_config)}
	
	def registerNodeClass(self, nodeclass):
		self.node_classes[nodeclass.nodetype] = nodeclass
		self.default_config.fields['pubsub#node_type'].addOption(nodeclass.nodetype, nodeclass.nodetype.title())
	
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
				item_id = nodei.publish(item, item_id)
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
	
	def handleGetDefaultConfig(self, stanza):
		xml = stanza.xml
		xml.attrib['to'] = xml.attrib['from']
		xml.attrib['from'] = self.xmpp.jid
		xml.attrib['type'] = 'result'
		default = xml.find('{http://jabber.org/protocol/pubsub#owner}pubsub/{http://jabber.org/protocol/pubsub#owner}default')
		default.append(self.default_config.getXML('form'))
		self.xmpp.send(xml)
	
	def handleCreateNode(self, stanza):
		xml = stanza.xml
		create = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}create')
		node = create.get('node', uuid.uuid4().hex)
		xform = xml.find('{http://jabber.org/protocol/pubsub}pubsub/{http://jabber.org/protocol/pubsub}configure/{jabber:x:data}x')
		if xform is None:
			logging.warning("No Config included")
			xform = self.default_config.getXML('submit')
		config = self.xmpp.plugin['xep_0004'].buildForm(xform)
		config = self.default_config.merge(config)
		values = config.getValues()
		nodeclass = self.node_classes.get(values['pubsub#node_type'])
		if node in self.nodes or nodeclass is None:
			self.xmpp.send(self.xmpp.makeIqError(xml.get('id')))
			return
		self.nodes[node] = nodeclass(self, self.db, node, config)
		iq = self.xmpp.makeIqResult(xml.get('id'))
		iq.attrib['from'] = self.xmpp.jid
		iq.attrib['to'] = xml.get('from')
		pubsub = ET.Element('{http://jabber.org/protocol/pubsub}pubsub')
		create = ET.Element('{http://jabber.org/protocol/pubsub}create', {'node': node})
		pubsub.append(create)
		iq.append(pubsub)
		self.xmpp.send(iq)
