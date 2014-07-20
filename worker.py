from redis import Redis
import os, urlparse, json, logging, threading
import requests

#TODO move config out. This is a mess make this into proper package with config and test config
url = urlparse.urlparse(os.environ.get('REDIS_HOST'))
queue_key = os.environ.get('REDIS_QUEUE_KEY')
public_api = os.environ.get('PUBLIC_TITLES_API')
private_api  = os.environ.get('PRIVATE_TITLES_API')
search_api = os.environ.get('SEARCH_API')

#TODO when config cleaned and package created can move this to init?
queue = Redis(host=url.hostname, port=url.port, password=url.password)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

def extract_private(message):
        payload = message[1].replace('\'', '\"')
        json_data = json.loads(payload)
        return json_data

def extract_public(message):
    json_data = extract_private(message)
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


class Worker(object):

    def __init__(self, feeds, extract_data):
        self.feeds = feeds
        self.extract_data = extract_data

    def do_work(self, message):
        data = self.extract_data(message)
        logger.info("Extracted data %s from message %s" % (data, message))
        for feed in self.feeds:
            try:
                self.send(feed, data)
            except RuntimeError as e:
                logger.error("Encountered error %s'" % e)

    def send(self, feed_url, data):
        headers = {"Content-Type": "application/json"}
        full_url = "%s/%s" % (feed_url, data['title_number'])
        try:
            r = requests.put(full_url,  data=json.dumps(data), headers=headers)
            logger.info("PUT data %s to URL %s : status code %s'" %  (data, full_url, r.status_code))
        except requests.exceptions.RequestException as e:
            logger.error("Error sending %s to %s: Error %s" % (data, full_url, e))
            raise RuntimeError


class ConsumerThread(threading.Thread):

    def __init__(self, queue, queue_key, workers):
        threading.Thread.__init__(self)
        self.queue = queue
        self.queue_key = queue_key
        self.workers = workers

    def run(self):
        while True:
            logger.info("Public titles worker awaiting data from  %s'" %  self.queue)
            message = self.get_next_message()
            logger.info("Public titles worker received data %s from  %s'" %  (message, self.queue))
            for worker in workers:
                thread = threading.Thread(target=worker.do_work, args=(message,))
                thread.start()

    def get_next_message(self):
        return self.queue.blpop(self.queue_key)


if __name__ == '__main__':

    public_feeds = [public_api, search_api]
    private_feeds = [private_api]

    workers = [Worker(public_feeds, extract_public), Worker(private_feeds, extract_private)]

    public_titles_thread = ConsumerThread(queue, queue_key, workers)
    public_titles_thread.start()

