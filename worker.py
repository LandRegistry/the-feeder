from redis import Redis
import os, urlparse, json, logging, threading
import requests

#TODO move config out
# make this into proper package with config and test config
url = urlparse.urlparse(os.environ.get('REDIS_HOST'))
queue_key = os.environ.get('REDIS_QUEUE_KEY')
queue = Redis(host=url.hostname, port=url.port, password=url.password)
titles_api_url = os.environ.get('TITLES_API_URL')
search_api_url = os.environ.get('SEARCH_API_URL')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
#TODO clean up logging a bit

class PublicTitlesWorker(object):

    def __init__(self, queue,queue_key, destinations):
        self.queue = queue
        self.queue_key = queue_key
        self.destinations = destinations

    def do_work(self):
        logger.info("Public titles worker awaiting data from  %s'" %  self.queue)
        message = self.queue.blpop(self.queue_key)
        logger.info("Public titles thread received data %s from  %s'" %  (message, self.queue))
        for destination in self.destinations:
            data = self.build_public_data(message)
            try:
                self.send(destination, data)
            except Exception, e:
                logger.error("Encountered error %s'" % e)

    def send(self, destination, data):
        title_url = "%s/%s" % (destination, data['title_number'])
        headers = {"Content-Type": "application/json"}
        r = requests.put(title_url,  data=json.dumps(data), headers=headers)
        logger.info("PUT data %s to URL %s : status code %s'" %  (data, title_url, r.status_code))

    def build_public_data(self, message):
        payload = message[1].replace('\'', '\"')
        json_data = json.loads(payload)
        title_number = json_data.get('title_number')
        property_details = json_data.get('property')
        address = property_details.get('address')
        payment = json_data.get('payment')

        public_title = {"title_number": title_number,
              "house_number" : address.get('house_number'),
              "road" : address.get('road'),
              "town" : address.get('town'),
              "postcode" : address.get('postcode'),
              "price_paid": payment.get('price_paid')
        }
        return public_title


class WorkerThread(threading.Thread):

    def __init__(self, worker):
        threading.Thread.__init__(self)
        self.worker = worker

    def run(self):
        while True:
            self.worker.do_work()

if __name__ == '__main__':

    destinations = [titles_api_url]

    public_titles_thread = WorkerThread(PublicTitlesWorker(queue, queue_key, destinations))
    public_titles_thread.start()

