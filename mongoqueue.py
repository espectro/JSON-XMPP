#   Copyright 2012 Andrey Aleksandrov
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import pymongo

from time import time
import traceback

class Queue(object):

    def __init__(self, collection, timeout=300, max_attempts=3):
      self.collection = collection
      self.timeout = timeout
      self.max_attempts = max_attempts

    def close(self):
      # Close the in memory queue connection.
      self.collection.connection.close()

    def clear(self):
      # Clear the queue.
      return self.collection.drop()
      
    def size(self):
      # Total size of the queue
      return self.collection.count()

    def count(self, query = { "_reserved": { '$exists': True } }):
      # By default, the surprising number of reserved tasks in the queue
      return self.collection.find(query).count()

    def add(self, task = {}, opts = {"priority": int(time()), "attempts": 0, "errors" : []}):
      task.update(opts)
      self.collection.insert(task)

    def reserve(self, opts = {"priority": int(time()), "attempts": 0, "errors" : []}):
      result = self.collection.find_and_modify(
          query = {
            "priority": { '$lte': opts["priority"] },
            "_reserved": { '$exists': False },
          },
          sort = { "priority": 1 },
          update = { '$set': { "_reserved": int(time()) } }
      )
      return result

    def reschedule(self, task, opts = {"priority": -1, "attempts": -1, "errors" : []}):
      if opts["priority"] < 0:
        opts["priority"] = task["priority"]
      if opts["attempts"] < 0:
        opts["attempts"] = task["attempts"] + 1
      self.collection.update(
        { "_id": task["_id"] },
        {
          '$unset': { "_reserved": 0 },
          '$set'  : { "priority": opts["priority"], "attempts": opts["attempts"] }
        })

    def remove(self, task):
      self.collection.remove( { "_id": task["_id"] } )

    def time_out(self, delay = 120):
      cutoff = int(time()) - delay
      self.collection.update(
        { "_reserved": { '$lt': cutoff } },
        { '$unset': { "_reserved": 0 } },
        safe = True, multi = True)
