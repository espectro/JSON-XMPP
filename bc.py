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

    #send(sender, gcm_json.encode('utf-8'))

def send(to, message):
  template = ("<message from=\"{0}\" to=\"{1}@{2}/{3}\" type=\"chat\"><html xmlns=\"http://jabber.org/protocol/xhtml-im\"><body>{4}</body></html></message>")
  connection.send(xmpp.protocol.Message(
    node=template.format(connection.Bind.bound[0], to["user"], to["domain"], to["ressource"], str(message))))

jid = xmpp.JID(user)
connection = xmpp.Client(server, debug=[]) #
connection.connect()
auth = connection.auth(jid.getNode(), password, ressource)
if not auth:
  print 'Authentication failed!'
  sys.exit(1)

connection.RegisterHandler('message', message_callback)

connection.sendInitPresence()

i = 1
while True:
  connection.Process(1)
  out = outbox.reserve()
  if out != None:
    send(out["sender"],json.dumps(out["message"]))
    outbox.remove(out)
  i = i + 1
	
