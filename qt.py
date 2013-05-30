import pymongo
from mongoqueue import queue

connection = pymongo.MongoClient("mongodb://localhost")
db = connection.xmpp

q = queue(db.queue)
q.clear()
print q.add({"content": {"key":111}})
q.add({"content": {"key":222}})
q.add({"content": {"key":333}})

task = q.reserve()
q.error(task, {"text":"qdfwsdfsd", "pid":8888, "code":-123})
print q.reschedule(task)

task = q.reserve()
q.remove(task)

q.reserve()
q.reserve()

q.timeout(-1)

print q.size()
print q.count()
