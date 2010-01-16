import logging
from xml.etree import cElementTree as ET 

class PubsubAdhoc(object):
	def __init__(self, pubsub):
		self.xmpp = pubsub.xmpp
		self.ps = pubsub
		self.about = "Create and configure nodes, leafs, and add items."
		self.pubsub = self.xmpp.plugin['xep_0060']
		self.xform = self.xmpp.plugin['xep_0004']
		self.adhoc = self.xmpp.plugin['xep_0050']
		
		createleaf = self.xmpp.plugin['xep_0004'].makeForm('form', "Create Leaf")
		createleaf.addField('node', 'text-single')
		self.xmpp.plugin['xep_0050'].addCommand('newleaf', 'Create Leaf', createleaf, self.createLeafHandler, True)

		#createcollect = self.xmpp.plugin['xep_0004'].makeForm('form', "Create Collection")
		#createcollect.addField('node', 'text-single', 'Node name')
		#self.xmpp.plugin['xep_0050'].addCommand('newcollection', 'Create Collection', createcollect, self.createCollectionHandler, True)

		setitem = self.xmpp.plugin['xep_0004'].makeForm('form', "Set Item")
		setitem.addField('node', 'text-single')
		setitem.addField('id', 'text-single')
		setitem.addField('xml', 'text-multi')
		self.xmpp.plugin['xep_0050'].addCommand('setitem', 'Set Item', setitem, self.setItemHandler, True)

		remitem = self.xmpp.plugin['xep_0004'].makeForm('form', "Retract Item")
		remitem.addField('node', 'text-single', 'Node name')
		remitem.addField('id', 'text-single')
		self.xmpp.plugin['xep_0050'].addCommand('remitem', 'Retract Item', remitem, self.retractItemHandler, True)

		
		confnode = self.xmpp.plugin['xep_0004'].makeForm('form', "Configure Node")
		confnode.addField('node', 'text-single')
		self.xmpp.plugin['xep_0050'].addCommand('confnode', 'Configure Node', confnode, self.updateConfigHandler, True)
		
		subnode = self.xmpp.plugin['xep_0004'].makeForm('form', "Subscribe Node")
		subnode.addField('node', 'text-single')
		subnode.addField('jid', 'text-single')
		self.xmpp.plugin['xep_0050'].addCommand('subnode', 'Subscribe Node', subnode, self.subscribeNodeHandler, True)
	
		delnode = self.xmpp.plugin['xep_0004'].makeForm('form', "Delete Node")
		delnode.addField('node', 'text-single')
		self.xmpp.plugin['xep_0050'].addCommand('delnode', 'Delete Node', delnode, self.deleteNodeHandler, True)

		affiliation = self.xmpp.plugin['xep_0004'].makeForm('form', "Change Affiliation")
		affiliation.addField('node', 'text-single', 'Node name')
		affiliation.addField('jid', 'text-single')
		affs = affiliation.addField('affiliation', 'list-single', 'Affilation')
		affs_list = ('owner', 'publisher', 'member', 'none', 'outcast')
		for aff in affs_list:
			affs.addOption(aff, aff.title())
		self.xmpp.plugin['xep_0050'].addCommand('affiliation', 'Change Affiliation', affiliation, self.setAffiliation, True)
		
	def getStatusForm(self, title, msg):
		status = self.xform.makeForm('form', title)
		status.addField('done', 'fixed', value=msg)
		return status

	def createLeafHandler(self, form, sessid):
		value = form.getValues()
		node = value.get('node')
		self.adhoc.sessions[sessid]['pubsubnode'] = node
		nodeform = self.ps.default_config
		if nodeform:
			return nodeform, self.createLeafHandlerSubmit, True
	
	def createLeafHandlerSubmit(self, form, sessid):
		if not self.ps.createNode(self.adhoc.sessions[sessid]['pubsubnode'], form, who=self.adhoc.sessions[sessid]['jid']):
			return self.getStatusForm('Error', "Could not create node."), None, False
		else:
			return self.getStatusForm('Done', "Node %s created." % self.adhoc.sessions[sessid]['pubsubnode']), None, False
	
	def createCollectionHandler(self, form, sessid):
		value = form.getValues()
		node = value.get('node')
		self.adhoc.sessions[sessid]['pubsubnode'] = node
		self.xmpp.plugin['xep_0060'].create_node(self.psserver, node, collection=True)
		nodeform = self.pubsub.getNodeConfig(self.psserver, node)
		if nodeform:
			return nodeform, self.updateConfigHandler, True
	
	def subscribeNodeHandler(self, form, sessid):
		value = form.getValues()
		node = value.get('node')
		jid = value.get('jid')
		if self.ps.subscribeNode(node, jid, who=self.adhoc.sessions[sessid]['jid'], to=self.adhoc.sessions[sessid]['to']):
			return self.getStatusForm('Done', "Subscribed to node %s." % node), None, False
		return self.getStatusForm('Error', "Could not subscribe to %s." % node), None, False
	
	def deleteNodeHandler(self, form, sessid):
		value = form.getValues()
		node = value.get('node')
		if self.pubsub.deleteNode(self.psserver, node):
			return self.getStatusForm('Done', "Deleted node %s." % node), None, False
		return self.getStatusForm('Error', "Could not delete %s." % node), None, False
	
	def updateConfigHandler(self, form, sessid):
		value = form.getValues()
		node = value.get('node')
		self.adhoc.sessions[sessid]['pubsubnode'] = node
		nodeform = self.ps.getNodeConfig(node)
		if nodeform:
			return nodeform, self.updateConfigHandlerSubmit, True
		else:
			return self.getStatusForm('Error', 'Unable to retrieve node configuration.'), None, False
	
	def updateConfigHandlerSubmit(self, form, sessid):
		node = self.adhoc.sessions[sessid]['pubsubnode']
		self.ps.configureNode(node, form)
		return self.getStatusForm('Done', "Updated node %s." % node), None, False
	
	
	def setItemHandler(self, form, sessid):
		value = form.getValues()
		self.ps.publish(value['node'], ET.fromstring(value['xml']), value['id'], self.adhoc.sessions[sessid]['jid'])
		done = self.xform.makeForm('form', "Finished")
		done.addField('done', 'fixed', value="Published Item.")
		return done, None, False
	
	def retractItemHandler(self, form, sessid):
		value = form.getValues()
		self.pubsub.deleteItem(self.psserver, value['node'], value['id'])
		done = self.xform.makeForm('form', "Finished")
		done.addField('done', 'fixed', value="Retracted Item.")
		return done, None, False
	
	def setAffiliation(self, form, sessid):
		value = form.getValues()
		self.pubsub.modifyAffiliation(self.psserver, value['node'], value['jid'], value['affiliation'])
		done = self.xform.makeForm('form', "Finished")
		done.addField('done', 'fixed', value="Updated Affiliation.")
		return done, None, False
