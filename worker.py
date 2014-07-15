from redis import Redis
import os
import urlparse

url = urlparse.urlparse(os.environ.get('REDIS_HOST'))
redis_queue = os.environ.get('REDIS_QUEUE_KEY')
redis = Redis(host=url.hostname, port=url.port, password=url.password)

def process_queue_items():
    while 1:
        msg = redis.blpop(redis_queue)
        try:
            print msg
            # do some real work
        except Exception, e:
            print e

if __name__ == '__main__':
    process_queue_items()
