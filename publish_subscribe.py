import os
import sleekxmpp.componentxmpp
import logging
import sleekpubsub
import sleekpubsub.jobnode

import configparser
from optparse import OptionParser
#import sleekxmpp.xmlstream.xmlstream

#sleekxmpp.xmlstream.xmlstream.HANDLER_THREADS = 5

if __name__ == '__main__':
	#parse command line arguements

	import os
	optp = OptionParser()
	optp.add_option('-q','--quiet', help='set logging to ERROR', action='store_const', dest='loglevel', const=logging.ERROR, default=logging.INFO)
	optp.add_option('-d','--debug', help='set logging to DEBUG', action='store_const', dest='loglevel', const=logging.DEBUG, default=logging.INFO)
	optp.add_option('-v','--verbose', help='set logging to COMM', action='store_const', dest='loglevel', const=5, default=logging.INFO)
	optp.add_option("-c","--config", dest="configfile", default="config.ini", help="set config file to use")
	opts,args = optp.parse_args()

	config = configparser.RawConfigParser()
	config.read(opts.configfile)
	
	f = open(config.get('pubsub', 'pid'), 'w')
	f.write("%s" % os.getpid())
	f.close()
	
	logging.basicConfig(level=opts.loglevel, format='%(levelname)-8s %(message)s')
	xmpp = sleekxmpp.componentxmpp.ComponentXMPP(config.get('pubsub', 'host'), config.get('pubsub', 'secret'), config.get('pubsub', 'server'), config.getint('pubsub', 'port'))
	xmpp.registerPlugin('xep_0004')
	xmpp.registerPlugin('xep_0030')
	xmpp.registerPlugin('xep_0045')
	xmpp.registerPlugin('xep_0050')
	xmpp.registerPlugin('xep_0060')
	pubsub = sleekpubsub.PublishSubscribe(xmpp, config.get('pubsub', 'dbfile'))
	pubsub.registerNodeType(sleekpubsub.jobnode)
	if xmpp.connect():
		xmpp.process(threaded=False)
	else:
		print("Unable to connect.")
