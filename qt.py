import pymongo
from mongoqueue import Queue

connection = pymongo.MongoClient("mongodb://localhost")
db = connection.xmpp
tasks = db.queue

q = Queue(tasks)

q.add({"content": {"key":111}})
q.add({"content": {"key":222}})
q.add({"content": {"key":333}})

task = q.reserve()
q.reschedule(task)

task = q.reserve()
q.remove(task)

q.reserve()
q.reserve()

q.time_out()

print q.size()
print q.count()