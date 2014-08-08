import os
import sys
import time
import json
import requests
import cPickle as pickle

from . import logger
from . import queue
from . import queue_key
from . import public_search_api
from . import authenticated_search_api
from . import geo_api
from raven.base import Client

def authenticated_filter(message):
    # presumably return the data "as-is"
    return pickle.loads(message[1])

def public_filter(message):
    """Removing sensitive data"""

    depickled = pickle.loads(message[1])
    depickled.pop('proprietors', None)
    return depickled

def geo_filter(message):

    result = {}
    depickled = pickle.loads(message[1])
    result['title_number'] = depickled['title_number']
    result['extent'] = depickled['extent']

    return result

class Worker(object):

    def __init__(self, feed_url, filter):
        self.feed_url = feed_url
        self.filter = filter

    def do_work(self, message):
        try:
            data = self.filter(message)
            logger.info("Filtered data %s from message %s" % (data, message))
            self.send(data)
        except pickle.UnpicklingError as e:
            logger.error("Error extracting data from %s : Error %s" % (message, e))

    def send(self, data):
        headers = {"Content-Type": "application/json"}
        try:
            payload = json.dumps(data)

            #todo: all apis should accept put at /titles
            if '<title_number>' in self.feed_url:
                self.feed_url = self.feed_url.replace('<title_number>', data['title_number'])

            response = requests.put(self.feed_url,  data=payload, headers=headers)
            logger.info("PUT data %s to URL %s : status code %s'" %  (data, self.feed_url, response.status_code))
        except requests.exceptions.RequestException as e:
            logger.error("Error sending %s to %s: Error %s" % (data, self.feed_url, e))
        except:
            e = sys.exc_info()[0]
            logger.error("Error extracting data from %s : Error %s" % (data, e))

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
                logger.info("Worker awaiting data from  %s" %  self.queue)
                message = self.get_next_message()
                logger.info("Worker received data %s from  %s" %  (message, self.queue))
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


if __name__ == '__main__':

    workers = []
    workers.append(Worker(public_search_api, public_filter))
    workers.append(Worker(authenticated_search_api, authenticated_filter))
    workers.append(Worker(geo_api, geo_filter))    

    consumer = Consumer(queue, queue_key, workers)
    consumer.run()
