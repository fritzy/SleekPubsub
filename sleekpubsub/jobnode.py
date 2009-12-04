from . node import BaseNode
from . node import Item
from . node import ItemEvent
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.handler.callback import Callback
from xml.etree import cElementTree as ET


class JobItem(Item):
	states = ('new', 'claimed', 'processing', 'finished')
	def __init__(self, *args, **dargs):
		Item.__init__(self, *args, **dargs)
		self.state = 'new'
		self.level = 0
		self.worker = ''
		self.result = ''
	
	def setState(self, state):
		if state in self.states:
			self.state = state
	
	def getState(self):
		return self.state
	
	def isState(self, state):
		return self.state == state
	
	def setWorker(self, worker):
		self.worker = worker
	
	def getWorker(self):
		return worker
	
	def setResult(self, result):
		print("Setting up result!")
		self.result = result
	
	def getResult(self):
		return result
	
class JobNode(BaseNode):
	nodetype = 'job'

	def __init__(self, *args, **dargs):
		BaseNode.__init__(self, *args, **dargs)
		self.item_class = JobItem
	
	def eachJobListener(self, item):
		return [item.who]
	
	def notifyState(self, event, state):
		item_id = event.item.name
		payload = event.item.result
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = ET.Element('{http://andyet.net/protocol/pubsubjob#event}pubsubjob', {'node': self.name, 'item': item_id, 'worker': event.item.worker, 'state': state})
		if payload != '':
			xevent.append(payload)
		msg.append(xevent)
		for jid in self.eachJobListener(event.item): 
			if not event.hasJid(jid):
				event.addJid(jid)
				msg.attrib['to'] = jid
				self.xmpp.send(msg)

	def notifyClaimed(self, event):
		item_id = event.item.name
		payload = event.item.payload
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = ET.Element('{http://andyet.net/protocol/pubsubjob}claimed', {'node': self.name, 'item': item_id, 'worker': event.item.worker})
		msg.append(xevent)
		for jid in self.eachJobListener(event.item): 
			if not event.hasJid(jid):
				event.addJid(jid)
				msg.attrib['to'] = jid
				self.xmpp.send(msg)
	
	def notifyProcessing(self, event):
		item_id = event.item.name
		payload = event.item.payload
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = ET.Element('{http://andyet.net/protocol/pubsubjob}processed', {'node': self.name, 'item': item_id, 'worker': event.item.worker})
		msg.append(xevent)
		for jid in self.eachJobListener(event.item): 
			if not event.hasJid(jid):
				event.addJid(jid)
				msg.attrib['to'] = jid
				self.xmpp.send(msg)
	
	def notifyCancelled(self, event):
		item_id = event.item.name
		payload = event.item.payload
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = ET.Element('{http://andyet.net/protocol/pubsubjob}cancelled', {'node': self.name, 'item': item_id, 'worker': event.item.worker})
		msg.append(xevent)
		for jid in self.eachJobListener(event.item): 
			if not event.hasJid(jid):
				event.addJid(jid)
				msg.attrib['to'] = jid
				self.xmpp.send(msg)
	
	def notifyResult(self, event):
		item_id = event.item.name
		payload = event.item.payload
		jid=''
		msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.jid)
		xevent = event.item.result
		xevent.attrib = {'node': self.name, 'item': item_id, 'worker': event.item.worker}
		msg.append(xevent)
		for jid in self.eachJobListener(event.item): 
			if not event.hasJid(jid):
				event.addJid(jid)
				msg.attrib['to'] = jid
				self.xmpp.send(msg)

class JobNodeExtension(BaseNode):
	
	def __init__(self, pubsub):
		self.pubsub = pubsub
		self.pubsub.registerNodeClass(JobNode)
		self.xmpp = pubsub.xmpp
	#	self.xmpp.registerHandler(Callback('job acquire', MatchXMLMask("<iq xmlns='%s' type='get'><pubsubjob xmlns='http://andyet.net/protocol/pubsubjob' /></iq>" % self.xmpp.default_ns), self.handleJobAcquire))
	#	self.xmpp.registerHandler(Callback('job process', MatchXMLMask("<iq xmlns='%s' type='set'><pubsubjob xmlns='http://andyet.net/protocol/pubsubjob'><process xmlns='http://andyet.net/protocol/pubsubjob' /></pubsubjob></iq>" % self.xmpp.default_ns), self.handleJobProcess))
	#	self.xmpp.registerHandler(Callback('job cancel', MatchXMLMask("<iq xmlns='%s' type='set'><pubsubjob xmlns='http://andyet.net/protocol/pubsubjob'><cancel xmlns='http://andyet.net/protocol/pubsubjob'  /></pubsubjob></iq>" % self.xmpp.default_ns), self.handleJobCancel))
	#	self.xmpp.registerHandler(Callback('job result', MatchXMLMask("<iq xmlns='%s' type='set'><pubsubjob xmlns='http://andyet.net/protocol/pubsubjob'><result xmlns='http://andyet.net/protocol/pubsubjob'  /></pubsubjob></iq>" % self.xmpp.default_ns), self.handleJobResult))
		self.xmpp.registerHandler(Callback('job setstate', MatchXMLMask("<iq xmlns='%s' type='set'><pubsubjob xmlns='http://andyet.net/protocol/pubsubjob' /></iq>" % self.xmpp.default_ns), self.handleJobState))
	
	def handleJobState(self, stanza):
		xml = stanza.xml
		ifrom = xml.get('from')
		pubsubjob = xml.find('{http://andyet.net/protocol/pubsubjob}pubsubjob')
		node_name = pubsubjob.get('node')
		item_id = pubsubjob.get('item')
		newstate = pubsubjob.get('state')
		node = self.pubsub.nodes.get(node_name)
		#TODO check to see if ifrom is subscribed
		if node is not None and node.nodetype == 'job' and item_id in node.items:
			item = node.items[item_id]
			if newstate == 'claimed' and item.isState('new'):
				#make sure they're a member
				#allow them to acquire the job
				item.setState(newstate)
				item.setWorker(ifrom)
				node.notifyState(ItemEvent(node, item), 'claimed')
				iq = self.xmpp.makeIqResult(xml.get('id'))
			elif newstate == 'processing' and item.isState('claimed') and ifrom == item.worker:
				item.setState('processing')
				node.notifyState(ItemEvent(node, item), 'processing')
				iq = self.xmpp.makeIqResult(xml.get('id'))
			elif newstate == 'finished' and item.isState('processing'):
				#make sure they're the ones that claimed the job
				#publish result notice
				#accept finished
				item.setState('finished')
				if pubsubjob.getchildren():
					result = pubsubjob.getchildren()[0]
					item.setResult(result)
				node.notifyState(ItemEvent(node, item), 'finished')
				node.deleteItem(item.name)
				iq = self.xmpp.makeIqResult(xml.get('id'))
			elif newstate == 'cancelled':
				pass
				#make sure they're the job owner or the job publisher
			else:
				iq = self.xmpp.makeIqError(xml.get('id'))
				#send some sort of error
		else:
			iq = self.xmpp.makeIqError(xml.get('id'))
		iq.attrib['to'] = ifrom
		iq.attrib['from'] = xml.get('to')
		iq.append(pubsubjob)
		self.xmpp.send(iq)
	
	def handleJobAcquire(self, stanza):
		xml = stanza.xml
		ifrom = xml.get('from')
		pubsubjob = xml.find('{http://andyet.net/protocol/pubsubjob}pubsubjob')
		node_name = pubsubjob.get('node')
		item_id = pubsubjob.get('item')
		node = self.pubsub.nodes.get(node_name)
		#TODO check to see if ifrom is subscribed
		if node is not None and node.nodetype == 'job' and item_id in node.items and node.items[item_id].isState('new'):
			item = node.items[item_id]
			item.setState('claimed')
			item.setWorker(ifrom)
			node.notifyClaimed(ItemEvent(node, item))
			iq = self.xmpp.makeIqResult(xml.get('id'))
		else:
			iq = self.xmpp.makeIqError(xml.get('id'))
		iq.attrib['to'] = ifrom
		iq.attrib['from'] = xml.get('to')
		iq.append(pubsubjob)
		self.xmpp.send(iq)

	def handleJobProcess(self, stanza):
		xml = stanza.xml
		ifrom = xml.get('from')
		pubsubjob = xml.find('{http://andyet.net/protocol/pubsubjob}pubsubjob')
		node_name = pubsubjob.get('node')
		item_id = pubsubjob.get('item')
		node = self.pubsub.nodes.get(node_name)
		if node is not None and node.nodetype == 'job' and item_id in node.items and node.items[item_id].isState('claimed') and ifrom == node.items[item_id].worker:
			item = node.items[item_id]
			item.setState('processing')
			node.notifyProcessing(ItemEvent(node, item))
			iq = self.xmpp.makeIqResult(xml.get('id'))
		else:
			print((ifrom, node.items[item_id].worker))
			iq = self.xmpp.makeIqError(xml.get('id'))
		iq.attrib['to'] = ifrom
		iq.attrib['from'] = xml.get('to')
		iq.append(pubsubjob)
		self.xmpp.send(iq)

	def handleJobCancel(self, stanza):
		xml = stanza.xml
		ifrom = xml.get('from')
		pubsubjob = xml.find('{http://andyet.net/protocol/pubsubjob}pubsubjob')
		node_name = pubsubjob.get('node')
		item_id = pubsubjob.get('item')
		node = self.pubsub.nodes.get(node_name)
		if node is not None and node.nodetype == 'job' and item_id in node.items and ifrom == node.items[item_id].worker:
			item = node.items[item_id]
			item.setState('new')
			item.setWorker(None)
			item.level += 1 
			node.notifyCancelled(ItemEvent(node, item))
			node.notifyItem(ItemEvent(node, item))
			iq = self.xmpp.makeIqResult(xml.get('id'))
		else:
			iq = self.xmpp.makeIqError(xml.get('id'))
		iq.attrib['to'] = ifrom
		iq.attrib['from'] = xml.get('to')
		iq.append(pubsubjob)
		self.xmpp.send(iq)

	def handleJobResult(self, stanza):
		xml = stanza.xml
		ifrom = xml.get('from')
		pubsubjob = xml.find('{http://andyet.net/protocol/pubsubjob}pubsubjob')
		result = pubsubjob.find('{http://andyet.net/protocol/pubsubjob}result')
		node_name = pubsubjob.get('node')
		item_id = pubsubjob.get('item')
		node = self.pubsub.nodes.get(node_name)
		if node is not None and node.nodetype == 'job' and item_id in node.items and node.items[item_id].isState('processing') and ifrom == node.items[item_id].worker:
			item = node.items[item_id]
			item.setState('finished')
			item.setResult(result)
			node.notifyResult(ItemEvent(node, item))
			node.deleteItem(item.name)
			iq = self.xmpp.makeIqResult(xml.get('id'))
		else:
			iq = self.xmpp.makeIqError(xml.get('id'))
		iq.attrib['to'] = ifrom
		iq.attrib['from'] = xml.get('to')
		iq.append(pubsubjob)
		self.xmpp.send(iq)

extension_class = JobNodeExtension
