import os
import sleekxmpp.componentxmpp
from optparse import OptionParser
import logging
import sleekpubsub
#import sleekxmpp.xmlstream.xmlstream

#sleekxmpp.xmlstream.xmlstream.HANDLER_THREADS = 5

if __name__ == '__main__':
	#parse command line arguements

	import os
	f = open('pubsub.pid', 'w')
	f.write("%s" % os.getpid())
	f.close()
	optp = OptionParser()
	optp.add_option('-q','--quiet', help='set logging to ERROR', action='store_const', dest='loglevel', const=logging.ERROR, default=logging.INFO)
	optp.add_option('-d','--debug', help='set logging to DEBUG', action='store_const', dest='loglevel', const=logging.DEBUG, default=logging.INFO)
	optp.add_option('-v','--verbose', help='set logging to COMM', action='store_const', dest='loglevel', const=5, default=logging.INFO)
	optp.add_option("-c","--config", dest="configfile", default="config.xml", help="set config file to use")
	opts,args = optp.parse_args()
	
	logging.basicConfig(level=opts.loglevel, format='%(levelname)-8s %(message)s')
	xmpp = sleekxmpp.componentxmpp.ComponentXMPP('pubsub.debian', 'secreteating', 'localhost', 5230)
	xmpp.registerPlugin('xep_0004')
	xmpp.registerPlugin('xep_0030')
	xmpp.registerPlugin('xep_0045')
	xmpp.registerPlugin('xep_0050')
	xmpp.registerPlugin('xep_0060')
	xmpp.registerPlugin('xep_0199')
	pubsub = sleekpubsub.PublishSubscribe(xmpp)
	if xmpp.connect():
		xmpp.process(threaded=False)
	else:
		print("Unable to connect.")
