import os
from raven.base import Client

from thefeeder import logger


class Consumer(object):
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
                logger.info("Worker received data %s from  %s" % (message, self.queue))
                self.send_to_workers(message)
            except Exception:
                if in_heroku:
                    client.captureException()
                raise

    def get_next_message(self):
        return self.queue.blpop(self.queue_key)

    def send_to_workers(self, message):
        for worker in self.workers:
            worker.do_work(message)