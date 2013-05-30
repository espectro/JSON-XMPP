#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re, json, xmpp, pymongo
from mongoqueue import queue

connection = pymongo.MongoClient("mongodb://localhost")
db = connection.xmpp           # attach to db
cnf = db.config                # specify the colllection

inbox  = queue(db.inbox)
outbox = queue(db.outbox)

inbox.clear()
outbox.timeout(-1)

try:
    cid = cnf.find_one()
  
    user      = cid["user"]
    password  = cid["password"]
    server    = cid["server"]
    ressource = cid["ressource"]
    print server
except:
    print "Error trying to read collection:" + sys.exc_info()[0]
    sys.exit(1)

# Unique message id for downstream messages
sent_message_id = 0

def message_callback(session, message):
    global sent_message_id
    content = message.getTags('body')
    if len(content) > 0:
        try:
            msg = json.loads(content[0].getData())
            mime = 'application/json'
        except:
            msg = content[0].getData()
            mime = 'text/plain'
        m = re.search('(.+)@(.+)/(.+)', str(message.getAttr('from'))) 
        inbox.add( {
          "sender"  : { "user" : m.group(1), "domain": m.group(2), "ressource": m.group(3) },
          "mime"    : mime,
          "message" : msg
        })

def send(to, message):
    template = ("<message from=\"{0}\" to=\"{1}@{2}/{3}\" type=\"chat\"><html xmlns=\"http://jabber.org/protocol/xhtml-im\"><body>{4}</body></html></message>")
    connection.send(xmpp.protocol.Message(
      node=template.format(connection.Bind.bound[0], to["user"], to["domain"], to["ressource"], str(message))))

def post(out):
    if out != None:
      if out["mime"] == "application/json":
          send(out["sender"], json.dumps(out["message"]))
      else:
          send(out["sender"], out["message"].encode('utf-8'))
      outbox.remove(out)

def bot_loop(connection):
    while 1:
        try:
            while connection.Process(1): 
                post(outbox.reserve())
            connection.disconnect()
        except KeyboardInterrupt:
            sys.exit(0)
        #except IOError:
        except:
            connection.reconnectAndReauth()
            connection.sendInitPresence()
            bot_loop(connection)

def reconnect(connection):
    connection.reconnectAndReauth()

JID = xmpp.JID(user)
connection = xmpp.Client(server) #, debug=[]
connection.connect()
conres = connection.connect()
if not conres:
    print 'Cannot connect to server'
    sys.exit(1)
auth = connection.auth(JID.getNode(), password, ressource)
if not auth:
    print 'Authentication failed!'
    sys.exit(1)

connection.RegisterDisconnectHandler(reconnect)
connection.RegisterHandler('message', message_callback)
connection.sendInitPresence()
bot_loop(connection)