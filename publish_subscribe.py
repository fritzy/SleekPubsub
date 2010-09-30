import os
import sys
import sleekxmpp.componentxmpp
import logging
import logging.handlers
import sleekpubsub
#import sleekpubsub.jobnode

try:
	import configparser
except ImportError:
	import ConfigParser as configparser

from optparse import OptionParser

logging.addLevelName(5, "VERBOSE")
#import sleekxmpp.xmlstream.xmlstream

#sleekxmpp.xmlstream.xmlstream.HANDLER_THREADS = 5

#Default daemon parameters
#File mode creation mask of the daemon
UMASK = 0

# Default working directory for the daemon
WORKDIR = sys.path[0]

# Default maximum for the number of available file descriptors.
MAXFD = 1024

# The standard I/O file descriptors are redirected to /dev/null by default.
if (hasattr(os, "devnull")):
	REDIRECT_TO = os.devnull
else:
	REDIRECT_TO = "/dev/null"

def createDaemon():
	"""Detach this process from the controlling terminal and run it in the background as a daemon."""

	try:
		pid = os.fork()
	except OSError as e:
		raise Exception("%s [%d]" % (e.strerror, e.errno))

	if pid == 0: #The first child
		os.setsid() #Become session leader of this new session.  Also be guaranteed not to have a controlling terminal

		try:
			pid = os.fork() #Fork a second child
		except OSError as e:
			raise Exception("%s [%d]" % (e.strerror, e.errno))

		if pid == 0: #The second child
			os.chdir(WORKDIR) #TODO define WORKDIR in ini
			os.umask(UMASK) #TODO define UMASK in ini
		else:
			os._exit(0) #Exit parent (the first child) of the second child.
	else:
		os._exit(0) #Exit parent of the first child.

	import resource		# Resource usage information.
	maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
	if (maxfd == resource.RLIM_INFINITY):
		maxfd = MAXFD

	# Iterate through and close all file descriptors.
	for fd in range(0, maxfd):
		try:
			os.close(fd)
		except OSError:	# ERROR, fd wasn't open to begin with (ignored)
			pass

	os.open(REDIRECT_TO, os.O_RDWR)	# standard input (0)

	# Duplicate standard input to standard output and standard error.
	os.dup2(0, 1)			# standard output (1)
	os.dup2(0, 2)			# standard error (2)

	return(0)

if __name__ == '__main__':
	#parse command line arguements


	optp = OptionParser()
	optp.add_option('--daemon', action="store_true", dest='daemonize', help="run as daemon")
	optp.add_option('-q','--quiet', help='set logging to ERROR', action='store_const', dest='loglevel', const=logging.ERROR, default=None)
	optp.add_option('-d','--debug', help='set logging to DEBUG', action='store_const', dest='loglevel', const=logging.DEBUG, default=None)
	optp.add_option('-v','--verbose', help='set logging to COMM', action='store_const', dest='loglevel', const=5, default=None)
	optp.add_option("-c","--config", dest="configfile", default="config.ini", help="set config file to use")
	opts,args = optp.parse_args()

	retCode = 0
	if opts.daemonize:
		retCode = createDaemon()
	
	config = configparser.RawConfigParser()
	config.read(opts.configfile)
	
	loglevel = opts.loglevel or getattr(logging, config.get('settings', 'loglevel'))
	#print loglevel
	logfile = config.get('settings', 'logfile')

	if opts.daemonize:
		rootlogger = logging.getLogger('')
		rootlogger.setLevel(loglevel)
		formatter = logging.Formatter('%(levelname)-8s %(message)s')
		handler = logging.handlers.RotatingFileHandler(logfile)
		handler.setFormatter(formatter)
		rootlogger.addHandler(handler)
		logging.info("Daemonized")
	else:
		logging.basicConfig(level=loglevel, format='%(levelname)-8s %(message)s')
		logging.info("Not daemonized")

	f = open(config.get('pubsub', 'pid'), 'w')
	f.write("%s" % os.getpid())
	f.close()
	
	xmpp = sleekxmpp.componentxmpp.ComponentXMPP(config.get('pubsub', 'host'), config.get('pubsub', 'secret'), config.get('pubsub', 'server'), config.getint('pubsub', 'port'))
	xmpp.registerPlugin('xep_0004')
	xmpp.registerPlugin('xep_0030')
	xmpp.registerPlugin('xep_0045')
	xmpp.registerPlugin('xep_0050')
	xmpp.registerPlugin('xep_0060')
	pubsub = sleekpubsub.PublishSubscribe(xmpp, config.get('pubsub', 'dbfile'), config)
	#pubsub.registerNodeType(sleekpubsub.jobnode)

	if xmpp.connect():
		xmpp.process(threaded=False)
		xmpp.disconnect()
		logging.info("Saving...")
		pubsub.save()
		sys.exit(retCode)
	else:
		logging.log(logging.CRITICAL, "Unable to connect.")
