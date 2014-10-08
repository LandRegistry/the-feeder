import os
from raven.base import Client
import cPickle as pickle
from thefeeder import logger


class FeedConsumer(object):
    def __init__(self, queue, queue_key, workers):
        self.queue = queue
        self.queue_key = queue_key
        self.workers = workers

    def run(self):
        in_heroku = 'SENTRY_DSN' in os.environ
        if in_heroku:
            client = Client(dsn=os.environ['SENTRY_DSN'])
        while True:
            try:
                logger.info("Worker awaiting data from  %s" % self.queue)
                
                message = self.get_next_message()
                logger.info("Worker received a message %s from %s" % (message[0], self.queue))
                
                depickled = pickle.loads(message[1])
                logger.info("Depickled data: %s" % depickled)

                self.send_to_workers(depickled)
            except Exception:
                if in_heroku:
                    client.captureException()
                raise

    def get_next_message(self):
        return self.queue.blpop(self.queue_key)

    def send_to_workers(self, data):
        for worker in self.workers:
            worker.do_work(data)
