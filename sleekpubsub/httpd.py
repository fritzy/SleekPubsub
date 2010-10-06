from __future__ import with_statement
try:
	import _thread as thread
except ImportError:
	import thread
import sys

import logging, base64, time, os, socket, re, datetime, signal, traceback

if sys.version_info < (3,0):
	import urllib
else:
	import urllib.parse as urllib


try:
	from http.server import HTTPServer as BaseHTTPServer
except ImportError:
	from BaseHTTPServer import HTTPServer as BaseHTTPServer

try:
	from socketserver import BaseServer
except ImportError:
	from SocketServer import BaseServer

try:
	import http.server as httpserver
except ImportError:
	import BaseHTTPServer as httpserver

import json
from xml.etree import cElementTree as ET

class RESTHTTPServer(BaseHTTPServer):
	"""Extension on BaseHTTPServer that adds threading"""

	def __init__(self, server_address, RequestHandlerClass, maxthreads=100, pemfile=None, rest_handlers=None, userpass=None):
		try:
			if pemfile: #HTTPS server
				BaseServer.__init__(self, server_address, RequestHandlerClass)
				ctx = SSL.Context(SSL.SSLv23_METHOD)
				ctx.use_privatekey_file(pemfile)
				ctx.use_certificate_file(pemfile)
				self.socket = SSL.Connection(ctx, socket.socket(self.address_family, self.socket_type))
				self.server_bind()
				self.server_activate()
				self.httpstyle = "HTTPS"
			else: #HTTP server
				BaseHTTPServer.__init__(self, server_address, RequestHandlerClass)
				self.httpstyle = "HTTP"
		except socket.error as e:
			if e.errno == 98:
				logging.log(logging.ERROR, "Cannot start HTTP server, address already in use")
				return
			else:
				raise
		self.lock = thread.allocate_lock()
		self.maxthreads = maxthreads
		self.threads = 0
		self.rest_handlers = rest_handlers
		self.userpass = userpass

	def server_close(self):
		"""Closes down socket, parent server should have this called."""
		self.socket.close()

	def close_request(self, request):
		"""Called to close individual request."""
		#TODO track down why https takes 100% cpu on stale tcp sessions
		if hasattr(request, 'close'):
			request.close()

	def process_request(self, request, client_address):
		"""finish and close request, handling any errors encountered."""
		try:
			self.finish_request(request, client_address)
			self.close_request(request)
		except socket.error as e:
			if e.errno == 104:
				logging.log(logging.DEBUG, "Remote end closed connection.  Probably IE")
				self.close_request(request)
			else:
				raise
		except:
			self.handle_error(request, client_address)
			self.close_request(request)
		with self.lock:
			self.threads -= 1
		#print "closed request"

	def handle_request(self):
		"""Handle one request, possibly blocking."""
		try:
			request, client_address = self.get_request()
			logging.log(logging.DEBUG, "%s request on child %s" % (self.httpstyle, os.getpid()))
		except socket.error:
			return
		if self.verify_request(request, client_address):
			busy = True
			while busy:
				with self.lock:
					if self.threads < self.maxthreads:
						busy = False
				if busy:
					logging.log(logging.DEBUG, "Max threads encountered, waiting to handle request")
					time.sleep(.1)
			with self.lock:
				self.threads += 1
			logging.log(logging.DEBUG, "%s threads" % self.threads)
			#thread.start_new(self.process_request, (request, client_address))
			self.process_request(request, client_address)

httpserver.BaseHTTPRequestHandler.protocol_version = "HTTP/1.1"

class http_handler(httpserver.BaseHTTPRequestHandler):
	"""Extension on BaseHTTPRequestHandler.  This is our actual "server"."""

	def __init__(self, request, client_address, server):
		self.querystring = None
		self.postargs = None
		self.request_type = 'GET'
		httpserver.BaseHTTPRequestHandler.__init__(self, request, client_address, server)

	def setup(self):
		"""initial connection setup, setting rfile, wfile, and connection"""
		if hasattr(self.request, 'do_handshake'): #ssl mode
			self.connection = self.request
			self.rfile = socket._fileobject(self.request, "rb", self.rbufsize)
			self.wfile = socket._fileobject(self.request, "wb", self.wbufsize)
		else:
			httpserver.BaseHTTPRequestHandler.setup(self)

	def handle_one_request(self):
		"""Handle single HTTP request.  With SSL error catching."""
		try:
			httpserver.BaseHTTPRequestHandler.handle_one_request(self)
		#except SSL.ZeroReturnError:
		#	logging.log(logging.DEBUG, "SSL Connection closed cleanly.")
		#	self.close_connection = 1
		#except SSL.Error, strerror:
		#	import traceback
		#	logging.log(logging.ERROR, "SSL Error handling request: %s" % strerror)
		#	logging.log(logging.DEBUG, traceback.format_exc())
		#	self.close_connection = 1
		except socket.error as e:
			if e.errno == 104:
				logging.log(logging.DEBUG, "Connection reset by peer.  Ignoring.")
			else:
				raise
		except Exception as e:
			import traceback
			traceback.print_exc()
			logging.log(logging.ERROR, "Error handling request: %s" % "")
			logging.log(logging.DEBUG, traceback.format_exc())
			self.close_connection = 1
		self.close_connection = 1
	
	def do_GET(self):
		logging.debug("Doing a GET")
		if self.server.userpass is not None:
			username, password = self._getAuth()
			if not self.server.userpass == (username, password):
				return self._401Page()
		domain, controller, robject, args = self._parsePath(self.path)
		logging.debug((domain, controller, robject, args))
		if self.postargs:
			args.update(self.postargs)
		self.postargs = None
		try:
			handler = self.server.rest_handlers.get(controller, False)
			if handler:
				pointer = getattr(handler, "do_%s" % self.request_type)
				result, content_type = pointer(domain, controller, robject, args)
				result = str(result)
				self.send_response(200, "Ok")
				self.send_header("Content-Type", content_type)
				self.send_header("Content-Length", len(result))
				self.send_header("Cache-Control", "no-cache")
				self.end_headers()
				if sys.version_info < (3,0):
					self.wfile.write(result)
				else:
					self.wfile.write(bytes(result, 'utf8'))
			else:
				self._404Page()
		except socket.error as e:
			if e.errno == 32:
				logging.log(logging.DEBUG, "Socket closed during http response.")
			else:
				logging.log(logging.WARNING, "Socket error #%s. %s" % (e.errno, e.strerror))
		except:
			self._500Page(traceback.format_exc())
		finally:
			self.request_type = 'GET'
			return True
	
	def do_POST(self):
		"""Called when we receive a POST.  Translate encoding, decoding variables and then calling do_GET"""
		self.request_type = 'POST'
		line = self.headers.get('CONTENT-TYPE', None)
		if not line:
			logging.log(logging.DEBUG, "Bad POST request: missing content-type header")
			return self.send_message(412, "Missing content type header")
		plist = [x.strip() for x in line.split(';')]
		ctype = plist.pop(0).lower()
		pdict = {}
		for p in plist:
			i = p.find('=')
			if i >= 0:
				name = p[:i].strip().lower()
				value = p[i+1:].strip()
				if len(value) >= 2 and value[0] == value[-1] == '"':
					value = value[1:-1]
					value = value.replace('\\\\', '\\').replace('\\"', '"')
				pdict[name] = value
		if ctype == 'multipart/form-data':
			import cgi
			self.postargs = cgi.parse_multipart(self.rfile, pdict)
			if self.postargs is None:
				return self.send_error(501, "Invalid Mulitpart Request")
			qs = ""
		elif ctype == "application/x-www-form-urlencoded":
			clength = int(self.headers.get('CONTENT-LENGTH', -1))
			if clength < 0:
				logging.log(logging.DEBUG, "Bad POST request: missing content-length header")
				return self.send_error(411, "Missing content length header")
			qs = self.rfile.read(clength)
		elif ctype == 'text/xml':
			clength = int(self.headers.get('CONTENT-LENGTH', -1))
			qs = self.rfile.read(clength)
			qs = ET.fromstring(qs)
		elif ctype == "text/json":
			clength = int(self.headers.get('CONTENT-LENGTH', -1))
			qs = self.rfile.read(clength)
			qs = json.loads(qs.decode('utf8'))
		else:
			logging.log(logging.WARNING, "Invalid Content-Type: %s" % ctype)
			return self.send_error(501, "Content-Type %s not implemented" % ctype)
		self.querystring = qs
		logging.log(logging.DEBUG, "POST request querystring %s" % qs)
		return self.do_GET()
	
	def do_PUT(self):
		self.request_type = 'PUT'
		return self.do_GET()
	
	def do_DELETE(self):
		self.request_type = 'DELETE'
		return self.do_GET()
		
	def _parsePost(self):
		"""Parse out POST variables into named dict"""
		data = self.querystring
		if not isinstance(data, str) or not isinstance(data, bytes):
			return {'__data__': data}
		vars = {}
		for set in data.split('&'):
			name, value = set.split('=')
			value = urllib.unquote(value.replace('+', ' '))
			name = urllib.unquote(name)
			if '[' in name:
				name, index = name[:-1].split('[')
				if not vars.has_key(name):
					vars[name] = {}
				vars[name][index] = value
			else:
				vars[name] = value
		return vars
	
	def _parsePath(self, path):
		"""Parses path into action, data, and args"""
		domain = self.headers.get("Host")
		if domain:
			domain = domain.split(":",1)[0]
		path = self.path
		if path.count("?"):
			l, r = self.path.split('?', 1)
			args = self._parseGet(r)
			path = l
		else:
			args = {}
		if self.querystring is not None:
			args.update(self._parsePost())
		action, data = self.getAction(path)
		data = urllib.unquote(data)
		return (domain, action, data, args)
		
	def getAction(self, path):
		"""figure out action from request path"""
		path = path.lstrip("/")
		if path.count("/"):
			path = path.split("/")
			return '/'.join(path[:-1]), path[-1]
		else:
			return None, path
	
	def _parseGet(self, getstring):
		"""Parse out GET variables into named dict"""
		if getstring.find("&#38;") > -1:
			getstring = getstring.replace("&#38;","&") 
		argsets = getstring.split('&')
		vars = {}
		for argset in argsets:
			if argset:
				if "=" in argset:
					var, value = argset.split('=', 1)
					vars[urllib.unquote(var)] = urllib.unquote(value)
				else:
					vars[urllib.unquote(argset)] = None
		return vars
	
	def _301Page(self, redirect):
		"""Show 301 redirect page"""
		msg = """Page has moved <a href="%s">here</a>.""" % redirect
		self.send_response(301, 'Moved Permanently')
		self.send_header('Location', redirect)
		self.send_header("Content-Length", len(msg))
		self.end_headers()
		self.wfile.write(btyes(msg, 'utf8'))

	def _401Page(self, logout=None):
		"""Show 401 page"""
		self.send_response(401, 'Unauthorized')
		msg = """<html><body>You do not have permission to view this page.</body></html>"""
		if logout:
			msg = "<html><body>Logged out.  This window can now be closed.</body></html>"
		realm = "Pubsub"
		self.send_header('WWW-Authenticate', 'Basic realm="%s"' % realm)
		self.send_header("Content-type", "text/html")
		self.send_header("Content-Length", len(msg))
		self.end_headers()
		if sys.version_info < (3,0):
			self.wfile.write(msg)
		else:
			self.wfile.write(bytes(msg, 'utf8'))

	def _404Page(self):
		"""Show 404 page"""
		self.send_error(404, "Unknown page")
	
	def _406Page(self):
		"""Show 406 page"""
		self.send_error(406, "Not acceptable")

	def _500Page(self, msg="Internal Error"):
		"""Show 500 page"""
		self.send_error(500, msg)
	
	def _getAuth(self):
		"""parses out username and password from http request"""
		auth = self.headers.get('Authorization')
		username = password = None
		if auth and auth.startswith("Basic "):
			authstring = auth.partition(" ")[2]
			if sys.version_info < (3,0):
				authstring = base64.decodestring(authstring)
			else:
				authstring = base64.decodebytes(bytes(authstring, 'utf8'))
			if authstring.count(b":") == 1:
				username, password = authstring.split(b":")
				username = urllib.unquote(username.decode('utf8'))
				password = urllib.unquote(password.decode('utf8'))
		if username is not None and password is not None:
			return username, password
		return None, None


class RestHandler(object):
	def __init__(self, application=None):
		self.app = application
	
	def do_POST(self, domain, controller, obj, args):
		return 'This object does not support POST.', 'text/plain'
	
	def do_GET(self, domain, controller, obj, args):
		return 'This object does not support GET.', 'text/plain'
	
	def do_PUT(self, domain, controller, obj, args):
		return 'This object does not support PUT.', 'text/plain'
	
	def do_DELETE(self, domain, controller, obj, args):
		return 'This object does not support DELETE.', 'text/plain'

class DefaultHandler(RestHandler):
	
	def do_GET(self, domain, controller, obj, args):
		form = self.app.pubsub.getDefaultConfig()
		if not form:
			return json.dumps({'error': True}), 'text/plain'
		return json.dumps(form.getValues()), 'text/plain'

class NodeHandler(RestHandler):
	
	def do_GET(self, domain, controller, obj, args):
		form = self.app.pubsub.getNodeConfig(obj)
		if not form:
			return json.dumps({'error': True}), 'text/plain'
		return json.dumps(form.getValues()), 'text/plain'
	
	def do_POST(self, domain, controller, obj, args):
		form = self.app.pubsub.getNodeConfig(obj)
		if not form:
			form = self.app.pubsub.getDefaultConfig()
			form.setValues(args.get('__data__', {}))
			self.app.pubsub.createNode(obj, form, who=self.app.jid)
			return json.dumps({'node': obj, 'action': 'added'}), 'text/plain'
		form.setValues(args.get('__data__', {}))
		self.app.pubsub.configureNode(obj, form)
		return json.dumps({'node': obj, 'action': 'configured'}), 'text/plain'
	
	def do_DELETE(self, domain, controller, obj, args):
		if self.app.pubsub.deleteNode(obj):
			return json.dumps({'error': False}), 'text/plain'
		else:
			return json.dumps({'error': True}), 'text/plain'
		
class SubscribeHandler(RestHandler):
	def do_GET(self, domain, controller, obj, args):
		logging.debug("Handling subscription")
		try:
			self.app.pubsub.subscribeNode(obj, args['jid'], to=args.get('to'), who=self.app.jid)
			logging.debug("Done, son")
			return json.dumps({'error': False}), 'text/plain'
		except:
			logging.error(traceback.format_exc())
		return json.dumps({'error': True}), 'text/plain'

class UnSubscribeHandler(RestHandler):
	def do_GET(self, domain, controller, obj, args):
		if self.app.pubsub.unsubscribeNode(obj, args['jid'], subid=args.get('subid')):
			return json.dumps({'error': False}), 'text/plain'
		else:
			return json.dumps({'error': True}), 'text/plain'

class PublishHandler(RestHandler):
	def do_POST(self, domain, controller, obj, args):
		item_id = self.app.pubsub.publish(obj, args.get('__data__'))
		if item_id:
			return json.dumps({'error': False, 'item_id': item_id}), 'text/plain'
		else:
			return json.dumps({'error': True}), 'text/plain'
		
class AffiliationHandler(RestHandler):
	def do_POST(self, domain, controller, obj, args):
		worked = self.app.pubsub.modifyAffiliations(obj, args.get('__data__'), who=self.app.jid)
		return json.dumps({'error': not(worked)}), 'text/plain'
	
	def do_GET(self, domain, controller, obj, args):
		results = self.app.pubsub.getAffiliations(obj, who=self.app.jid)
		if results:
			return json.dumps(results), 'text/plain'
		else:
			return json.dumps({'error': False}), 'text/plain'

class TestHandler(RestHandler):
	def do_POST(self, domain, controller, obj, args):
		return json.dumps({'error': False}), 'text/plain'
	
class HTTPD(object):
	def __init__(self, pubsub):
		self.pubsub = pubsub
		self.jid = self.pubsub.config['rest']['userasjid']
		self.rest_handlers = {
			"default": DefaultHandler(self),
			"node": NodeHandler(self),
			"subscribe": SubscribeHandler(self),
			"unsubscribe": UnSubscribeHandler(self),
			"publish": PublishHandler(self),
			"affiliation": AffiliationHandler(self),
			"test": TestHandler(self),
		}
		self.httpd = RESTHTTPServer((self.pubsub.config['rest']['server'], self.pubsub.config['rest']['port']), http_handler, rest_handlers=self.rest_handlers, userpass=(self.pubsub.config['rest']['user'], self.pubsub.config['rest']['passwd']))
		thread.start_new(self.process_request, tuple())
	
	def process_request(self):
		while True:
			self.httpd.handle_request()
