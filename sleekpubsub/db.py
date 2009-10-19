
class PubsubDB(object):
	
	def createNode(self, node, config):
		pass
	
	def updateNodeConfig(self, node, config):
		pass
	
	def delNode(self):
		pass
	
	def getNode(self):
		pass
	
	def getNodeConfig(self, node):
		pass
	
	def getNodes(self):
		pass
	
	def createItem(self):
		pass
	
	def delItem(self):
		pass
	
	def getItems(self, node):
		return {}
	
	def setAffiliation(self):
		pass
	
	def getAffiliations(self, node):
		return {'owner': ['fritzy@brokt.com'], 'publisher': [], 'member': [], 'outcast': [], 'subscriber': [('1', 'fritzy@netflint.net'), ('2', 'fritzy@brokt.com')], 'pending': []}

