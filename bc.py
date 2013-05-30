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
    print "Error trying to read collection:", sys.exc_info()[0]
    sys.exit(1)

# Unique message id for downstream messages
sent_message_id = 0

def recipient(uri):
    try:
        u = re.search('(.+)@(.+)/(.+)', uri) 
        result = { "user" : u.group(1), "domain": u.group(2), "ressource": u.group(3) }
    except:
        u = re.search('(.+)@(.+)', uri)
        result = { "user" : u.group(1), "domain": u.group(2) }
    return result
    
def receive(session, message):
    content = message.getTags('body')
    
    if len(content) > 0:
        try:
            msg = json.loads(content[0].getData())
            mime = 'application/json'
        except:
            msg = content[0].getData()
            mime = 'text/plain'
        
        inbox.add({
          "message": {
            "from"    : recipient(str(message.getAttr('from'))),
            "to"      : recipient(str(message.getAttr('to'))),
            "id"      : str(message.getAttr('id')),
            "mime"    : mime,
            "content" : msg
          }
        })

def send(to, message):
    global sent_message_id
    sent_message_id += 1
    template = ("<message from=\"{0}\" to=\"{1}@{2}\" type=\"chat\" id=\"{3}\"><html xmlns=\"http://jabber.org/protocol/xhtml-im\"><body>{4}</body></html></message>")
    connection.send(xmpp.protocol.Message(
      node=template.format(connection.Bind.bound[0], to["user"], to["domain"], sent_message_id, str(message))))

def post(out):
    if out != None:
        message = out["message"]
        if message["mime"] == "application/json":
            send(message["to"], json.dumps(message["content"]))
        else:
            send(message["to"], message["content"].encode('utf-8'))
        outbox.remove(out)

def bot_loop(connection):
    while True:
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
connection = xmpp.Client(server, debug=[]) #
conres = connection.connect()
if not conres:
    print 'Cannot connect to server'
    sys.exit(1)
auth = connection.auth(JID.getNode(), password, ressource)
if not auth:
    print 'Authentication failed!'
    sys.exit(1)

connection.RegisterDisconnectHandler(reconnect)
connection.RegisterHandler('message', receive)
connection.sendInitPresence()
bot_loop(connection)