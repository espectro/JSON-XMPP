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

class Queue(object):

    def __init__(self, collection):
      self.collection = collection
      #self.timeout = timeout
      #self.max_attempts = max_attempts

    def close(self):
      # Close the in memory queue connection.
      self.collection.connection.close()

    def clear(self):
      # Clear the queue.
      return self.collection.drop()
      
    def size(self):
      # Total size of the queue
      return self.collection.count()

    def count(self, query = { "_r": { '$exists': True } }):
      # By default, the surprising number of reserved tasks in the queue
      return self.collection.find(query).count()

    def add(self, task = {}, opts = {"_p": int(time()), "_a": 0, "errors" : []}):
      task.update(opts)
      self.collection.insert(task)

    def reserve(self, priority = int(time())):
      result = self.collection.find_and_modify(
          query = {
            "_p": { '$lte': priority },
            "_r": { '$exists': False },
          },
          sort   = { "_p": 1 },
          update = { '$set': { "_r": int(time()) } }
      )
      return result

    def reschedule(self, task, opts = {"_p": -1, "_a": -1}):
      if opts["_p"] < 0:
        opts["_p"] = task["_p"]
      if opts["_a"] < 0:
        opts["_a"] = task["_a"] + 1
      self.collection.update(
        { "_id": task["_id"] },
        {
          '$unset': { "_r": 0 },
          '$set'  : { "_p": opts["_p"], "_a": opts["_a"] }
        })
        
    def error(self, task, message):
      self.collection.update(
        { "_id": task["_id"] },
        { '$push': { "errors": message } })
        
    def remove(self, task):
      self.collection.remove({ "_id": task["_id"] })

    def timeout(self, delay = 120):
      cutoff = int(time()) - delay
      self.collection.update(
        { "_r": { '$lt': cutoff } },
        { '$unset': { "_r": 0 } },
        safe = True, multi = True)
