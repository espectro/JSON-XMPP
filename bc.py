#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re, json, xmpp, pymongo
from mongoqueue import queue
from optparse import OptionParser

opt = OptionParser()
opt.add_option("-c", "--config", dest="config",
                  help="config file", metavar="CNF")

(options, args) = opt.parse_args()

if options.config == None:
    print "Error to read config file"
    sys.exit(1)

try:
    config = json.load(open(options.config, 'r'))
except:
    print sys.exc_info()[1]
    sys.exit(1)
    
print "Start as ", config["user"]

connection = pymongo.MongoClient("mongodb://localhost")
db  = connection.xmpp           # attach to db
cnf = db.config                 # specify the colllection

try:
    cid = cnf.find_one({"user": config["user"]})
  
    user      = cid["user"]
    password  = cid["password"]
    server    = cid["server"]
    ressource = cid["ressource"]
except:
    print sys.exc_info()
    sys.exit(1)

inbox  = queue(db.inbox)
outbox = queue(db[cid["outbox"]])

inbox.clear()
outbox.timeout(-1)

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
    body = message.getBody()
    if body != None:
        try:
            content = json.loads(body)
            mime = 'application/json'
        except:
            content = body
            mime = 'text/plain'
        
        inbox.add({
          "message": {
            "from"    : recipient(str(message.getFrom())),
            "to"      : recipient(str(message.getTo())),
            "id"      : str(message.getID()),
            "mime"    : mime,
            "type"    : message.getType(),
            "content" : content
          }
        })

def send(to, message):
    global sent_message_id
    sent_message_id += 1
    template = ("<message from=\"{msg_from}\" to=\"{user}@{domain}\" type=\"chat\" id=\"{msg_id}\"><body>{body}</body>\n<html xmlns=\"http://jabber.org/protocol/xhtml-im\"><body xmlns=\"http://www.w3.org/1999/xhtml\">{body}</body></html></message>")
    connection.send(xmpp.protocol.Message(
      node = template.format(
        msg_from = connection.Bind.bound[0], 
        user   = to["user"], 
        domain = to["domain"], 
        msg_id = sent_message_id, 
        body   = str(message))))

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
connection = xmpp.Client(server) #, debug=[]
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