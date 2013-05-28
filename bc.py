#!/usr/bin/env python

import sys, re, json, xmpp, pymongo

connection = pymongo.MongoClient("mongodb://localhost")
db = connection.xmpp           # attach to db
cnf = db.config         # specify the colllection
  
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

  gcm = message.getTags('body')
  if len(gcm) > 0:
    m = re.search('(.+)@(.+)/(.+)', str(message.getAttr('from')))
    sender = { "user" : m.group(1), "domain": m.group(2), "ressource": m.group(3) }
    gcm_json = gcm[0].getData()

    send(sender, gcm_json.encode('utf-8'))

def send(to, json_data):
  template = ("<message from=\"{0}\" to=\"{1}@{2}/{3}\" type=\"chat\"><html xmlns=\"http://jabber.org/protocol/xhtml-im\"><body>{4}</body></html></message>")
  connection.send(xmpp.protocol.Message(
    node=template.format(connection.Bind.bound[0], to["user"], to["domain"], to["ressource"], json_data)))

jid = xmpp.JID(user)
connection = xmpp.Client(server) #, debug=[]
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
  #  print i
  i = i + 1
	
