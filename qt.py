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



def main():
    parser = ArgumentParser('mongo-xmpp-bot')
    parser.add_argument("-c", "--config", dest='config', 
        required=True, help="Config is ini file which contains username in mongodb")
    options = parser.parse_args()
    config = ConfigObj(options.config, configspec=options.config+'.spec', interpolation=False, encoding='UTF8')
    validator = Validator()
    result = config.validate(validator)
    if result != True:
        print >> sys.stderr, 'Config file validation failed!'
        sys.exit(1)

    #connect2mongo
    config = config['mongodb']
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

    # make log
    logging.basicConfig(
        level = getattr(logging, 'DEBUG', None),
        format = '%(asctime)s %(levelname)-8s %(name)-10s %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        filename = '/tmp/tc.log'
    )
    log = logging.getLogger('qt'),
    
    db = client.xmpp
    q = Queue(client.xmpp['aaleksandrov'], log)
    msg = { "message": {
            "from"    : 'aaleksandrov@kp.local',
            "to"      : 'archi@kp.local',
            "mime"    : 'plain/text',
            "content" : 'It\'s 100% free, no registration required'
        }}
    q.add(msg)

if __name__ == "__main__":
    main()
