#!/usr/bin/env python
# -*- coding: utf-8 -*-


#   Copyright 2013 Andrey Aleksandrov and Nikolay Spiridonov
#   Издательский дом "Комсомольская правда"

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import sys
import re

import json
import xmpp
import pymongo
import logging
from mongoqueue  import Queue
from argparse    import ArgumentParser
from configobj   import ConfigObj
from validate    import Validator

SENT_MESSAGE_ID = 0

class JBot(object):
    def __init__(self, config, debug):
        logging.basicConfig(
            level = getattr(logging, config['logging']['level'].upper(), None),
            format = config['logging']['format'],
            datefmt = config['logging']['datetime'],
            filename = config['logging']['filename']
        )
        self.__dict__.update({
            'log': logging.getLogger('jbot'),
            'store': self._connect(config['mongodb']),
            'config': config,
            'user': '%s@%s' % (config["xmpp"]["user"], config["xmpp"]["host"])
        })
        self._init(debug)


    def _init(self, debug):
        self._inbox = Queue(self.store.xmpp.inbox, self.log)
        self._outbox = Queue(self.store.xmpp[self.config["xmpp"]["user"]], self.log)
        #self._inbox.clear()
        self._outbox.timeout(-1)
        
        JID = xmpp.JID(self.user)
        self.connection = xmpp.Client(JID.getDomain(), debug=debug)
        server = self.config["xmpp"]["server"]
        port = int(self.config["xmpp"]["port"])
        if self.connection.connect((server, port)) == '':
            self.log.error('Cannot connect to server %s port %d' % (server, port))
            sys.exit(1)

        if self.connection.auth(
                JID.getNode(),
                self.config["xmpp"]["password"],
                self.config["xmpp"]["user"]+'-' ) == None:
            self.log.error('Authentication %s failed!' % self.user)
            sys.exit(1)

        self.connection.RegisterDisconnectHandler(self.connection.reconnectAndReauth())
        self.connection.RegisterHandler('message', self.receive)
        self.connection.getRoster()
        self.connection.sendInitPresence()


    def _connect(self, config):
        try:
            client = pymongo.MongoClient( config["host"], config["port"] )
        except pymongo.errors.ConnectionFailure, messages:
            print 'Error:', messages
            sys.exit(1)
        if config["auth"]:
            auth = client[config["auth_db"]]
            try:
                auth.authenticate(config["user"], config["passwd"])
            except KeyError:
                print 'KeyError: Not authenticating!'
                sys.exit(1)
        return client


    def recipient(self, uri):
        if type(uri) == dict:
            return uri
        try:
            u = re.search('(.+)@(.+)/(.+)', uri) 
            result = { 
                "user" : u.group(1),
                "domain": u.group(2),
                "ressource": u.group(3)
            }
        except:
            u = re.search('(.+)@(.+)', uri)
            result = {
                "user" : u.group(1),
                "domain": u.group(2)
            }
        return result


    def receive(self, session, message):
        return

        body = message.getBody()
        if body != None:
            try:
                content = json.loads(body)
                mime = 'application/json'
            except:
                content = body
                mime = 'text/plain'
            
            self._inbox.add({
                "message": {
                    "from"    : self.recipient(str(message.getFrom())),
                    "to"      : self.recipient(str(message.getTo())),
                    "id"      : str(message.getID()),
                    "mime"    : mime,
                    "type"    : message.getType(),
                    "content" : content
                }
            })  


  # save({
  #   _p:233213,
  #   message:{
  #     to:'room2@muc.kp.local', 
  #     from:'pbot1@kp.local/pbot1-',
  #     content:'Message from bot mongo collection',
  #     mime:'application/json',
  #     type: 'groupchat'}
  # })
    def send(self, to, message, mtype):
        self.room(str(to['user']+'@'+to['domain']))
        self.connection.send(xmpp.protocol.Message(
            to=str(to['user']+'@'+to['domain']),
            body=unicode(message, 'unicode-escape'),
            typ=str(mtype),
            frm=self.connection.Bind.bound[0]
        ))


    def post(self, out):
        self.log.debug('q')
        if out != None:
            message = out["message"]
            self.log.debug('post message from:%s to:%s' % (message["from"], message["to"]))
            if message["mime"] == "application/json":
                self.send(self.recipient(
                    message["to"]),
                    json.dumps(message["content"]),
                    message["type"]
                )
            else:
                self.send(self.recipient(
                    message["to"]), 
                    message["content"],
                    message["type"]
                )
            self._outbox.remove(out)


    def room(self, room, nick=None, password=None):
        self.connection.send(xmpp.Presence(
            to='%s/%s' % (room, self.config["xmpp"]["user"]))
        )
    

    def run(self):
        while True:
            try:
                while self.connection.Process(1):
                    self.post(self._outbox.reserve())
                self.connection.disconnect()
            except KeyboardInterrupt:
                self.log.debug('Keyboard interrupt')
                sys.exit(0)


def main():
    parser = ArgumentParser('mongo-xmpp-bot')
    parser.add_argument(
        "-c", "--config",
        dest='config', 
        required=True,
        help="Config is ini file which contains username in mongodb"
    )
    parser.add_argument(
        "-v", "--verbose", dest='verbose', 
        help='Enable debugging on connection',
        action="store_true"
    )
    options = parser.parse_args()
    config = ConfigObj(
        options.config, 
        configspec=options.config+'.spec',
        interpolation=False,
        encoding='UTF8'
    )
    validator = Validator()
    result = config.validate(validator)
    if result != True:
        print >> sys.stderr, 'Config file validation failed!'
        sys.exit(1)
    verbose = ()
    if options.verbose:
        verbose = ['always']
    app = JBot(config, verbose)
    app.run()

if __name__ == "__main__":
    main()