import sleekxmpp.componentxmpp
from sleekxmpp.xmlstream.matcher.xmlmask import MatchXMLMask
from sleekxmpp.xmlstream.handler.callback import Callback
from sleekxmpp.plugins.xep_0004 import Form
from xml.etree import cElementTree as ET
import uuid
import logging
import pickle
import time
import random
import copy
import sleekxmpp.plugins.stanza_pubsub as pubsub

class StateMachine(object):
    def __init__(self, resource, ns):
        self.ns = ns
        self.resource = resource
        self.registers = {}
        self.statexml = None

    def registerStateCallback(self, current, new, callback):
        self.registers[(current, new)] = callback

    def setState(self, xml, who=None):
        current = None
        if self.statexml is not None:
            current = self.statexml.tag.split('}')[-1]
        xmlns = xml.tag.split('}')[-1]
        if (current, xmlns) not in self.registers:
            return False
        passed = self.registers[(current, xmlns)](xml, who)
        if passed:
            self.updateState(xml)
            self.resource.saveState(xml)
        else:
            self.rejectState(xml, who)
        return passed

    def updateState(self, xml, broadcast=True):
        self.statexml = xml
        if broadcast:
            self.resource.notifyState(xml)

    def getState(self):
        if self.statexml is None:
            return None
        return self.statexml.tag
    
    def getXML(self):
        return self.statexml


class Subscription(object):
    def __init__(self, node, jid=None, subid=None, config=None, to=None):
        self.node = node
        self.jid = jid
        self.subid = subid
        self.config = config
        self.to = to
    
    def set(self, jid, subid, config, to=None):
        self.jid = jid
        self.subid = subid
        self.config = config
        self.to = to or self.to
    
    def getto(self):
        return self.to
    
    def getjid(self):
        return self.jid
    
    def getid(self):
        return self.subid

    def getconfig(self):
        return self.config
    
class Event(object):
    def __init__(self, node):
        self.nodes = []
        self.jids = []
        self.originalnode = node
    
    def addJid(self, jid):
        self.jids.append(jid)
    
    def hasJid(self, jid):
        return jid in self.jids
    
    def addNode(self, node):
        self.nodes.append(node)
    
    def hasNode(self, node):
        return node in self.nodes
    
    def cleanup(self):
        self.jids = []
    
class ItemEvent(Event):
    def __init__(self, name, item):
        Event.__init__(self, name)
        self.item = [item]
    
    def getItem(self):
        return item[0]
    
    def addItem(self, item):
        self.item.append(item)

class QueueItemEvent(ItemEvent):
    def __init__(self, name, item, bcast=None, rotation=0):
        ItemEvent.__init__(self, name, item)
        self.bcast = bcast
        self.rotation = rotation

class DeleteEvent(ItemEvent):
    pass

class ConfigEvent(Event):
    pass

class Item(object):
    state = None

    def __init__(self, node, name, who, payload=None, config=None, state=None):
        self.node = node
        self.name = name
        self.who = who
        self.payload = payload
        self.config = config
        self.time = time.time()
        if state is not None:
            xmlns = state.tag.split("}")[0][1:]
            if xmlns in self.state:
                self.state[xmls].updateState(state,broadcast=False)
    
    def getpayload(self):
        return self.payload
    
    def gettime(self):
        return self.time
    
    def getwho(self):
        return self.who
    
    def setState(self, state, who):
        xmlns = state.tag.split("}")[0][1:]
        if xmlns not in self.state:
            return False
        return self.state[xmlns].setState(state, who)

    def saveState(self, xml):
        self.node._saveItemState(self.name, xml)
    
    def notifyState(self, xml):
        self.node.notifyItemState(xml, item_id=self.name)

class QueueItem(Item):
    def __init__(self, *args, **kwargs):
        self.claimed = ""
        self.state = {}
        self.state['http://andyet.net/protocol/pubsubqueue'] = StateMachine(self, 'http://andyet.net/protocol/pubsubqueue')
        self.state['http://andyet.net/protocol/pubsubqueue'].registerStateCallback(None, "claimed", self.handleClaim)
        self.state['http://andyet.net/protocol/pubsubqueue'].registerStateCallback("unclaimed", "claimed", self.handleClaim)
        self.state['http://andyet.net/protocol/pubsubqueue'].registerStateCallback("claimed", "unclaimed", self.handleUnclaim)
        super(QueueItem, self).__init__(*args, **kwargs)
    
    def handleClaim(self, request, who):
        if self.claimed:
            return False
        if who.bare in self.node.subscriptionsbyjid or who.full in self.node.subscriptionsbyjid:
            self.claimed = who.full
            return True
        return False
    
    def handleUnclaim(self, request, who):
        if not self.claimed:
            return False
        if who.full == self.claimed or who.bare in self.node.affiliation['owner']:
            self.claimed = ""
            return True
        return False

class JobQueueItem(Item):
    def __init__(self, *args, **kwargs):
        self.claimtime = None
        self.claimed = ""
        self.cancel_counter = 0
        self.state = {}
        self.state['http://andyet.net/protocol/pubsubjob'] = StateMachine(self, 'http://andyet.net/protocol/pubsubjob')
        self.state['http://andyet.net/protocol/pubsubjob'].registerStateCallback(None, "claimed", self.handleClaim)
        self.state['http://andyet.net/protocol/pubsubjob'].registerStateCallback("unclaimed", "claimed", self.handleClaim)
        self.state['http://andyet.net/protocol/pubsubjob'].registerStateCallback("claimed", "unclaimed", self.handleUnclaim)
        self.state['http://andyet.net/protocol/pubsubjob'].registerStateCallback("claimed", "finished", self.handleFinished)
        super(JobQueueItem, self).__init__(*args, **kwargs)
    
    def handleClaim(self, request, who):
        if self.claimed:
            return False
        if who is None or who.bare in self.node.subscriptionsbyjid or who.full in self.node.subscriptionsbyjid:
            self.claimed = who.full
            return True
        return False
    
    def handleUnclaim(self, request, who):
        if not self.claimed:
            return False
        if who is None or who.full == self.claimed or who.bare in self.node.affiliation['owner']:
            self.claimed = ""
            return True
        return False

    def handleFinished(self, request, who):
        if not self.claimed:
            return False
        if who is None or who.full != self.claimed:
            return False
        return True

class BaseNode(object):
    nodetype = 'leaf'
    affiliationtypes = ('owner', 'publisher', 'member', 'outcast', 'pending')

    default_config = Form(None, title='Leaf Config Form')
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
    default_config.addField('pubsub#max_items', label='Max # of items to persist', value='100')
    default_config.addField('pubsub#expire', label='Expire')
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
    model.addOption('on_sub_and_presence', 'On Subscription And Presence')
    default_config.addField('pubsub#presence_based_delivery', 'boolean', label='Deliver notification only to available users', value=False)
    del ntype
    del model

    item_class = Item
    itemevent_class = ItemEvent

    def __init__(self, pubsub, db, name, config=None, owner=None, fresh=False, use_db=True):
        self.new_owner = owner
        self.use_db = use_db
        self.fresh = fresh
        self.pubsub = pubsub
        self.xmpp = self.pubsub.xmpp
        self.db = db
        self.name = name
        self.collections = []
        self.config = config or copy.copy(self.default_config)
        self.subscription_form = {}
        self.publish_form = {}
        self.items = {}
        self.itemorder = []
        self.synch = True
        self.state = ''
        #self.affiliations = {'owner': [], 'publisher': [], 'member': [], 'outcast': [], 'pending': []}
        self.affiliations = {}
        for afftype in self.affiliationtypes:
            self.affiliations[afftype] = []
        self.subscriptions = {}
        self.subscriptionsbyjid = {}
        if self.new_owner is not None:
            self.affiliations['owner'].append(self.new_owner)
        self.dbLoad()
        self.lastsaved = time.time()
        if self.pubsub.config['settings']['node_creation'] == 'createonsubscribe':
            self.use_db = False

        self.updates_per_second = 0.0
        self.recent_updates = 0
        self.recent_update_time = time.time()
    
    def dbLoad(self):
        if not self.use_db:
            return
        if not self.fresh:
            self.affiliations = self.db.getAffiliations(self.name)
            self.items = self.db.getItems(self.name)
            self.config = pickle.loads(self.db.getNodeConfig(self.name))
            parentset = self._checkconfigcollections(self.config, False)
            if not parentset: logging.warning("Was not able to set all parents in %s" % self.name)
            subs = self.db.getSubscriptions(self.name)
            for jid, subid, config in subs:
                self.subscriptions[subid] = Subscription(self, jid, subid, config)
                self.subscriptionsbyjid[jid] = self.subscriptions[subid]
        else:
            self.db.createNode(self.name, self.config, self.affiliations, self.items)

    def dbDump(self, save=False):
        if not self.use_db:
            return
        if save:
            self.db.synch(self.name, pickle.dumps(self.config), self.affiliations, self.items, subscriptions=self.subscriptions)
        else:
            self.db._synch(self.name, pickle.dumps(self.config), self.affiliations, self.items, subscriptions=self.subscriptions)
        self.lastsaved = time.time()

    def save(self):
        if not self.use_db:
            return
        logging.info("Saving %s" % self.name)
        #self.dbDump(True)
        self.db._synch(self.name, pickle.dumps(self.config), self.affiliations, self.items, subscriptions=self.subscriptions, newdb=True)

    def discoverItems(self):
        pass
    
    def getSubscriptions(self):
        return self.subscriptions

    def getAffiliations(self):
        return self.affiliations
    
    def notifyItemState(self, xml, item_id=None, who=None):
        return
        msg = self.xmpp.Message()
        msg['psstate_event']['psstate']['node'] = self.name
        msg['psstate_event']['psstate']['item'] = item_id
        msg['psstate_event']['psstate']['payload'] = xml
        for step in self.eachSubscriber():
            for jid, mto in step:
                msg['from'] = mto
                msg['to'] = jid
                msg.send()
        for affiliation in self.affiliations:
            if affiliation == 'member':
                continue
            for barejid in self.affiliations[affiliation]:
                resources = self.xmpp.roster.get(barejid, {'presence': {}})['presence'].keys()
                if resources:
                    for resource in resources:
                        msg['from'] = self.xmpp.boundjid
                        msg['to'] = "%s/%s" % (barejid, resource)
                        msg.send()
                else:
                        msg['from'] = self.xmpp.boundjid
                        msg['to'] = barejid
                        msg.send()

    
    def subscribe(self, jid, who=None, config=None, to=None):
        if (
            (who is None or self.xmpp.getjidbare(who) in self.affiliations['owner'] or who.startswith(jid)) and 
            (self.config['pubsub#access_model'] == 'open' or 
                (self.config['pubsub#access_model'] == 'whitelist' and jid in self.affiliations['member']) or
                (self.xmpp.getjidbare(who) in self.affiliations['owner'])
            )
        ):
            if not self.config['sleek#dupesubscriptions'] and jid in self.subscriptionsbyjid:
                return False
            subid = uuid.uuid4().hex
            if config is not None:
                config = ET.tostring(config.getXML('submit'))
            self.subscriptions[subid] = Subscription(self, jid, subid, config, to)
            self.subscriptionsbyjid[jid] = self.subscriptions[subid]
            if self.config['sleek#saveonchange'] and self.use_db:
                self.db.addSubscription(self.name, jid, subid, config, to)
            if self.config['pubsub#send_last_published_item'] in ('on_sub', 'on_sub_and_presence'):
                if len(self.itemorder) > 0:
                    event = ItemEvent(self.name, self.items[self.itemorder[0]])
                    if len(self.itemorder) > 1:
                        for item in self.itemorder[1:]:
                            event.addItem(self.items[item])
                    self.notifyItem(event, jid)
            return subid
        else:
            return False
        #TODO modify affiliation

    def unsubscribe(self, jid, who=None, subid=None):
        if subid is None:
            try:
                subid = self.subscriptionsbyjid[jid].getid()
            except KeyError:
                return False
        if self.config['sleek#saveonchange'] and self.use_db:
            self.db.deleteSubscription(self.name, jid, subid)
        try:
            del self.subscriptions[subid]
            if self.subscriptionsbyjid[jid].getid() == subid:
                del self.subscriptionsbyjid[jid]
        except IndexError():
            return False
        #TODO add error cases
        #TODO add ACL
        return True
    
    def getSubscriptionOptions(self):
        pass

    def setSubscriptionOptions(self):
        pass
    
    def getItems(self, max=0):
        pass
    
    def getLastItem(self, node):
        pass
    
    def eachSubscriber(self, step=1, filterjid=None):
        "Generator for subscribers."
        if step < 1:
            raise ValueError
        subscriptions = self.subscriptions.keys()
        result = []
        idx = 0 # would enumerate, but num of results isn't necessary the same as len(subscriptions)
        for subid in subscriptions:
            subscriber = self.subscriptions[subid]
            jid = subscriber.getjid()
            mto = subscriber.getto()
            if not (filterjid is not None and (filterjid != jid)):
                if '/' in jid or not self.config.get('pubsub#presence_based_delivery', False):
                        result.append((jid, mto))
                        if idx % step == 0:
                            yield result
                            result = []
                        idx += 1
                else:
                    resources = self.xmpp.roster.get(jid, {'presence': {}})['presence'].keys()
                    random.shuffle(resources)
                    for resource in resources:
                        result.append(("%s/%s" % (jid, resource), mto))
                        if idx % step == 0:
                            yield result
                            result = []
                        idx += 1
    
    def publish(self, item, item_id=None, options=None, who=None):
        self.recent_updates += 1
        spent = time.time() - self.recent_update_time 
        if spent >= 10.0:
            self.updates_per_second = float(self.recent_updates) / spent
            self.recent_update_time = time.time()
            self.recent_updates = 1
        if not item_id:
            item_id = uuid.uuid4().hex
        self.xmpp.schedule("%s::%s::publish" % (self.name, item_id), 0, self._publish, (item,item_id, options, who))
        return item_id

    def _publish(self, item, item_id=None, options=None, who=None):
        if item.tag == '{http://jabber.org/protocol/pubsub}item':
            payload = item.getchildren()[0]
        else:
            payload = item
        item_inst = self.item_class(self, item_id, who, payload, options)
        if self.config.get('pubsub#persist_items', False):
            if self.config['sleek#saveonchange'] and self.use_db:
                self.db.setItem(self.name, item_id, payload)
            self.items[item_id] = item_inst
            if item_id not in self.itemorder:
                self.itemorder.append(item_id)
            else:
                self.itemorder.append(self.itemorder.pop(self.itemorder.index(item_id)))
        event = self.itemevent_class(self.name, item_inst)
        self.notifyItem(event)
        max_items = int(self.config.get('pubsub#max_items', 0))
        if max_items != 0 and len(self.itemorder) > max_items:
            self.deleteItem(self.itemorder[0])

    def deleteItem(self, id):
        if id in self.items:
            item = self.items[id]
            del self.items[id]
            self.itemorder.pop(self.itemorder.index(id))
            self.notifyDelete(ItemEvent(self.name, item))
        #TODO: DB
    
    def _checkconfigcollections(self, config, reconfigure=True):
        collections = []
        passed = True
        nodes = config.get('pubsub#collection', [])
        if type(nodes) != type([]):
            nodes = [nodes]
        for node in nodes:
            if node not in self.pubsub.nodes:
                passed = False
            else:
                collections.append(node)
        if not reconfigure or passed:
            self.collections = collections
        return passed
    
    def create(self, config=None):
        pass
    
    def getConfig(self, default=False):
        return self.config

    def configure(self, config):
        if not self._checkconfigcollections(config):
            raise XMPPError() #TODO make this the right error
        self.config.update(config)
        # we do this regardless of cache settings
        if self.use_db:
            self.db.synch(self.name, config=pickle.dumps(self.config))
    
    def setState(self, state, who):
        pass

    def setItemState(self, item_id, state, who=None):
        if item_id in self.items:
            return self.items[item_id].setState(state, who)
        return False
    
    def _saveState(self, xml):
        pass

    def _saveItemState(self, node, xml):
        pass

    
    def purgeNodeItems(self):
        pass
    
    def approvePendingSubscription(self, jid):
        pass

    def modifySubscriptions(self, jids={}):
        pass
    
    def modifyAffiliations(self, affiliations={}, who=None):
        if who is not None and who not in self.affiliations['owner']:
            return False
        for key in affiliations:
            if key not in self.affiliationtypes:
                return False
        self.affiliations.update(affiliations)
        if self.config['sleek#saveonchange'] and self.use_db:
            self.db.synch(self.name, affiliations=self.affiliations)
        return True
    
    def getAffiliations(self, who=None):
        if who is not None and who not in self.affiliations['owner']:
            return False
        return self.affiliations
    
    def notifyItem(self, event, filterjid=None):
        if event.hasNode(self.name):
            return False
        event.addNode(self.name)
        jid=''
        msg = self.xmpp.Message()
        msg['to'] = jid
        msg['from'] = self.xmpp.boundjid
        xevent = ET.Element('{http://jabber.org/protocol/pubsub#event}event')
        items = ET.Element('items', {'node': event.originalnode})
        for itemi in event.item:
            item_id = itemi.name
            payload = itemi.payload
            item = ET.Element('item', {'id': item_id})
            item.append(payload)
            items.append(item)
        xevent.append(items)
        if payload.tag == '{jabber:client}body':
            msg['body'] = payload.text
            msg['type'] = 'chat'
        else:
            msg.append(xevent)
        for toset in self.eachSubscriber(filterjid=filterjid): 
            jid, mto = toset[0]
            if not event.hasJid(jid):
                event.addJid(jid)
                msg['to'] = jid
                msg['from'] = mto or self.xmpp.boundjid
                self.xmpp.send(msg)
        for parent in self.collections:
            if parent in self.pubsub.nodes:
                self.pubsub.nodes[parent].notifyItem(event, jid)
    
    def notifyConfig(self):
        pass
    
    def notifyDelete(self, event):
        if event.hasNode(self.name):
            return False
        event.addNode(self.name)
        jid=''
        msg = self.xmpp.Message()
        msg['to'] = jid
        msg['from'] = self.xmpp.boundjid

        xevent = ET.Element('{http://jabber.org/protocol/pubsub#event}event')
        items = ET.Element('items', {'node': event.originalnode})
        item = ET.Element('retract', {'id': event.item[0].name})
        #item.append(payload)
        items.append(item)
        xevent.append(items)
        msg.append(xevent)
        for toset in self.eachSubscriber(): 
            jid, mto = toset[0]
            if not event.hasJid(jid):
                event.addJid(jid)
                msg['to'] = jid
                print "WHAT THE HELL IS", mto, type(mto)
                msg['from'] = mto or self.xmpp.boundjid
                self.xmpp.send(msg)
        for parent in self.collections:
            if parent in self.pubsub.nodes:
                self.pubsub.nodes[parent].notifyDelete(event)

    def delete(self):
        for sub in self.subscriptions.keys():
            del self.subscriptions[sub]
        for sub in self.subscriptionsbyjid.keys():
            del self.subscriptionsbyjid[sub]
        for item in self.items.keys():
            del self.items[item]
        for collection in self.collections:
            self.collections.pop(self.collections.index(collection))


class CollectionNode(BaseNode):

    def publish(self, *args, **kwargs):
        return False

    def deleteItem(self, *args, **kwargs):
        return False

class QueueNode(BaseNode):
    default_config = copy.copy(BaseNode.default_config)
    bcast = default_config.addField("queue#braodcast", "list-single", label="Broadcast behavior", value="broadcast")
    bcast.addOption('broadcast', 'Broadcast')
    bcast.addOption('roundrobin', "Round Robin")
    bcast.addOption('hybrid', 'Hybrid')
    del bcast
    default_config.addField('queue#bcaststep', label='Number of subscribers to step to in round robin', value='4')
    default_config.addField('queue#wait', label='Seconds to wait for subscribers to claim', value='4')
    item_class = QueueItem
    itemevent_class = QueueItemEvent

    def __init__(self, *args, **kwargs):
        BaseNode.__init__(self, *args, **kwargs)
        self.current_event = None
        self.current_iterator = None
        self.last_claim_jid = None

    def setState(self, state):
        super(QueueNode, self).setState(state)

    def setItemState(self, item_id, state, who=None):
        passed = super(QueueNode, self).setItemState(item_id, state, who)
        if passed and state.tag == "{http://andyet.net/protocol/pubsubqueue}claimed":
            self.last_claim_jid = who
            self.deleteItem(self.current_event.item.name)
            self.current_event = None
            if len(self.itemorder):
                event = self.itemevent_class(self.name, self.items[self.itemorder[0]])
                self.xmpp.schedule("%s::%s::bcast" % (self.name, item_id), 0, self.notifyItem, (event,))
        return passed
    
    def notifyItem(self, event):
        if self.current_event is not None and self.current_event != event:
            return
        if not self.current_event:
            self.current_event = event
        self.current_iterator = self.eachSubscriber()#step=int(self.config.get('queue#bcaststep', 1)))
        self._broadcast()
        
    def _broadcast(self):
        if self.current_event is None:
            return
        event = self.current_event
        item_id = event.item.name
        if item_id not in self.itemorder:
            self.current_event = None
            return
        payload = event.item.payload
        jid=''
        msg = self.xmpp.makeMessage(mto=jid, mfrom=self.xmpp.boundjid)
        xevent = ET.Element('{http://jabber.org/protocol/pubsub#event}event')
        items = ET.Element('items', {'node': event.originalnode})
        item = ET.Element('item', {'id': item_id})
        item.append(payload)
        items.append(item)
        xevent.append(items)
        if payload.tag == '{jabber:client}body':
            msg['body'] = payload.text
            msg['type'] = 'chat'
        else:
            msg.append(xevent)
        try:
            jidset = self.current_iterator.next()
            for jid, mto in jidset:
            #for step in self.eachSubscriber():
                #for jid, mto in step:
                msg.attrib['to'] = jid
                msg['from'] = mto or self.xmpp.boundjid
                self.xmpp.send(msg)
                #self.xmpp.schedule("%s::%s::bcast" % (self.name, item_id), 0, self._broadcast, tuple())
            self._broadcast()
        except StopIteration:
            self.xmpp.schedule("%s::%s::bcast" % (self.name, item_id), 20.0, self.notifyItem, (event,))

class JobNode(QueueNode):
    item_class = JobQueueItem

    def __init__(self, *args, **kwargs):
        QueueNode.__init__(self, *args, **kwargs)
        self.xmpp.schedule("%s::node_maintenance" % (self.name,),  5, self.maintenance, repeat=True)

    def maintenance(self):
        for item_id in self.items:
            item = self.items[item_id]
            #print "state of %s::%s = %s" % (self.name, item_id, item.state['http://andyet.net/protocol/pubsubjob'].getState())
            if item.state['http://andyet.net/protocol/pubsubjob'].getState() == "{http://andyet.net/protocol/pubsubjob}claimed":
                if item.claimtime is not None and time.time() - item.claimtime > 5.0:
                    logging.warning( "%s::%s timed out" % (self.name, item_id))
                    self.setItemState(item_id,  ET.Element("{http://andyet.net/protocol/pubsubjob}unclaimed"))
                
    def setItemState(self, item_id, state, who=None):
        passed = BaseNode.setItemState(self, item_id, state, who)
        if passed and state.tag == "{http://andyet.net/protocol/pubsubjob}claimed" and self.current_event.item.name == item_id:
            self.last_claim_jid = who
            #self.deleteItem(self.current_event.item.name)
            self.current_event = None
            self.items[item_id].claimtime = time.time()
            idx = self.itemorder.index(item_id)
            for nitem_id in self.itemorder:
                if self.items[nitem_id].state['http://andyet.net/protocol/pubsubjob'].statexml is None or self.items[nitem_id].state['http://andyet.net/protocol/pubsubjob'].statexml.tag == '{http://andyet.net/protocol/pubsubjob}unclaimed':
                    event = self.itemevent_class(self.name, self.items[nitem_id])
                    #self.xmpp.schedule("%s::%s::bcast" % (self.name, nitem_id), 0, self.notifyItem, (event,))
                    self.notifyItem(event)
        elif passed and state.tag == "{http://andyet.net/protocol/pubsubjob}finished":
            self.deleteItem(item_id)
            if not len(self.itemorder):
                self.current_event = None
        elif passed and state.tag == "{http://andyet.net/protocol/pubsubjob}unclaimed":
            self.items[item_id].cancel_counter += 1
            if self.items[item_id].cancel_counter >= 3:
                logging.error("Placing job in purgatory")
                #TODO: actually do that
            self.deleteItem(item_id)
            if not len(self.itemorder):
                self.current_event = None
        return passed

class JobNode2(QueueNode):
    default_config = copy.copy(BaseNode.default_config)
    default_config.addField('queue#wait', label='Seconds to wait for subscribers to claim', value='4')
    item_class = JobQueueItem
    itemevent_class = ItemEvent

    def __init__(self, *args, **kwargs):
        BaseNode.__init__(self, *args, **kwargs)
        self.xmpp.schedule("%s::node_maintenance" % (self.name,),  5, self.maintenance, repeat=True)
        self.last_update_time = None
        self.last_update_size = 0
        self.xmpp.schedule("%s::node_maintenance" % (self.name,),  5, self.maintenance, repeat=True) 

    def setItemState(self, item_id, state, who=None):
        passed = BaseNode.setItemState(self, item_id, state, who)
        if passed and state.tag == "{http://andyet.net/protocol/pubsubjob}finished":
            self.deleteItem(item_id)
            self.notifyItem(None)
        elif passed and state.tag == "{http://andyet.net/protocol/pubsubjob}unclaimed":
            #self.notifyItem(None)
            self.sendToPurgatory(item_id)
        return passed

    def getSize(self):
        #return len(self.items)
        count = 0
        for item_id in self.items:
            item = self.items[item_id]
            #print "state of %s::%s = %s" % (self.name, item_id, item.state['http://andyet.net/protocol/pubsubjob'].getState())
            if item.state['http://andyet.net/protocol/pubsubjob'].getState() in (None, "{http://andyet.net/protocol/pubsubjob}unclaimed"):
                count += 1
        return count
    
    def notifyItem(self, event):
        size = self.getSize()
        if size == 0 or self.last_update_size == 0 or self.last_update_time is None or time.time() - self.last_update_time > 3.0:
        #if (self.last_update_size == 0 and size != 0) or (self.last_update_size != 0 and size == 0) or self.last_update_time is None or (time.time() - self.last_update_time) > 3.0:
            self.last_update_time = time.time()
            self.last_update_size = size
            msg = self.xmpp.Message()
            item = pubsub.EventItem()
            item['id'] = uuid.uuid4().hex
            pl = ET.Element('{http://andyet.net/protocol/pubsubjob}queuestatus',{'size': "%s" % size, 'per_sec': "%s" % self.updates_per_second})
            item['payload'] = pl
            msg['pubsub_event']['items'].append(item)
            msg['pubsub_event']['items']['node'] = self.name
            for step in self.eachSubscriber():
                for mto, mfrom in step:
                    msg['from'] = mfrom or self.xmpp.boundjid
                    msg['to'] = mto
                    msg.send()
    
    def getItems(self, max=5, who=None):
        item = []
        state = ET.Element('{http://andyet.net/protocol/pubsubjob}claimed')
        count = 0
        for item_id in self.itemorder:
            #if self.items[item_id].state['http://andyet.net/protocol/pubsubjob'].getState() in (None, "{http://andyet.net/protocol/pubsubjob}unclaimed"):
            if self.setItemState(item_id, state, who=who):
                item.append(self.items[item_id])
                count += 1
                if count >= max:
                    break
        return item

    def sendToPurgatory(self, item_id):
        self.pubsub.publish('__purgatory__', self.items[item_id].payload, id="%s::%s" % (self.name, self.items[item_id].name))
        self.deleteItem(item_id)
        self.notifyItem(None)

    def maintenance(self):
       for item_id in self.itemorder:
           if time.time() - self.items[item_id].time > 10.0:
               self.pubsub.publish('__purgatory__', self.items[item_id].payload, id="%s::%s" % (self.name, self.items[item_id].name))
               self.deleteItem(item_id)
               self.notifyItem(None)
    
    def notifyDelete(self, event):
        pass # we don't notify retracts for job nodes
