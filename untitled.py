""" 
 db.outbox.save({
	"mime" : "application/json",
	"sender" : {
		"ressource" : "MacBook Pr9F93752E",
		"domain" : "gmail.com",
		"user" : "andrey.aleksandrov"
	},
	"message" : {"a":{"b":1}},
	"_e" : [ ],
	"_a" : 0,
	"_p" : 1369839283
})

db.config.drop()
db.config.save({
	"password"  : "uhoaSQVBH8mR",
	"ressource" : "oishi-",
	"server"    : "jabber.ru",
	"user"      : "oishi.kuranosuke.yoshikatsu@jabber.ru",
	"outbox"    : "oishi"
})
db.config.save({
	"password" : "348c7SfWwUHHA",
	"ressource" : "asano-",
	"server" : "gmail.com",
	"user" : "asano.takumi.no.kami.naganori@gmail.com",
	"outbox"    : "asano"
})

db.config.save({
	"password"  : "qazwiox",
	"ressource" : "pbot-",
	"server"    : "first.node.desk.kp.ru",
	"user"      : "pbot@kp.local",
	"outbox"    : "pbot"
})


db.asano.save({
	"message" : {
		"content" : {
   "firstName": "Иван",
   "lastName": "Иванов",
   "address": {
       "streetAddress": "Московское ш., 101, кв.101",
       "city": "Ленинград",
       "postalCode": 101101
   },
   "phoneNumbers": [
       "812 123-1234",
       "916 123-4567"
   ]
},
		"to" : {
			"domain" : "jabber.ru",
			"user" : "oishi.kuranosuke.yoshikatsu"
		},
		"id" : "E1B4D75C4",
		"mime" : "application/json"
	},
	"_e" : [ ],
	"_a" : 0,
	"_p" : 1369920881
})

db.asano.save({
	"message" : {
		"content" : "тут идёт пространное объяснение теории чёрных дыр",
		"to" : {
			"domain" : "gmail.com",
			"user" : "andrey.aleksandrov"
		},
		"id" : "E1B4D75C-235A-4F17-A33E-97DF8D5533A4",
		"mime" : "text/plain"
	},
	"_e" : [ ],
	"_a" : 0,
	"_p" : 1369920881
})

db.outbox.save({
"mime" : "application/json",
	"sender" : {
		"ressource" : "TalkGadgetw1B724D60",
		"domain" : "gmail.com",
		"user" : "feelmyimba"
	},
	"message" : {"a":{"b":1}},
	"_e" : [ ],
	"_a" : 0,
	"_p" : 1369914765
})

def remove_task {
  my ( $self, $name, $task ) = @_;
  $self->_coll( $name )->remove( { $ID => $task } );
}

def apply_timeout {
  my ( $self, $name, $timeout ) = @_;
  $timeout //= 120;
  my $cutoff = time() - $timeout;
  $self->_coll( $name )->update(
    { $RESERVED => { '$lt'     => $cutoff } },
    { '$unset'  => { $RESERVED => 0 } },
    { safe => $self->safe, multiple => 1 }
  );
}

def size {
  my ($self, $name) = @_;
  return $self->_coll( $name )->count;
}

def count {
  my ($self, $name, $query) = @_;
  return $self->_coll( $name )->count( $query );
}


    def size(self):
        """Total size of the queue
        """
        return self.collection.count()

    def repair(self):
        """Clear out stale locks.

        Increments per job attempt counter.
        """
        self.collection.find_and_modify(
            query={
                "locked_by": {"$ne": None},
                "locked_at": {
                    "$lt": datetime.now() - timedelta(self.timeout)}},
            update={
                "$set": {"locked_by": None, "locked_at": None},
                "$inc": {"attempts": 1}}
        )

    def put(self, payload):
        """Place a job into the queue
        """
        job = dict(DEFAULT_INSERT)
        job['payload'] = payload
        return self.collection.insert(job)

    def next(self):
        return self._wrap_one(self.collection.find_and_modify(
            query={"locked_by": None,
                   "locked_at": None,
                   "attempts": {"$lt": self.max_attempts}},
            update={"$set": {"attempts": 1,
                             "locked_by": self.consumer_id,
                             "locked_at": datetime.now()}},
            sort=[('priority', pymongo.DESCENDING)],
            new=1,
            limit=1
        ))

    def _wrap_one(self, data):
        return data and Job(self, data) or None

    def stats(self):
        """Get statistics on the queue.

        Use sparingly requires a collection lock.
        """

        js = """function queue_stat(){
        return db.eval(
        function(){
           var a = db.%(collection)s.count(
               {'locked_by': null,
                'attempts': {$lt: %(max_attempts)i}});
           var l = db.%(collection)s.count({'locked_by': /.*/});
           var e = db.%(collection)s.count(
               {'attempts': {$gte: %(max_attempts)i}});
           var t = db.%(collection)s.count();
           return [a, l, e, t];
           })}""" % {
               "collection": self.collection.name,
               "max_attempts": self.max_attempts
           }

        return dict(zip(
            ["available", "locked", "errors", "total"],
            self.collection.database.eval(js)))


class Job(object):

    def __init__(self, queue, data):
        """
        """
        self._queue = queue
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def payload(self):
        return self._data['payload']

    @property
    def job_id(self):
        return self._data["_id"]

    ## Job Control

    def complete(self):
        """Job has been completed.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            remove=True)

    def error(self, message=None):
        """Note an error processing a job, and return it to the queue.
        """
        self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {
                "locked_by": None, "locked_at": None, "last_error": message},
                    "$inc": {"attempts": 1}})

    def progress(self, count=0):
        """Note progress on a long running task.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"progress": count, "locked_at": datetime.now()}})

    def release(self):
        """Put the job back into_queue.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"locked_by": None, "locked_at": None},
                    "$inc": {"attempts": 1}})

    ## Context Manager support

    def __enter__(self):
        return self.data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)
