from redis import Redis
import os
import urlparse
import requests
import json

url = urlparse.urlparse(os.environ.get('REDIS_HOST'))
redis_queue = os.environ.get('REDIS_QUEUE_KEY')
redis = Redis(host=url.hostname, port=url.port, password=url.password)

def process_queue_items():
    while 1:
        try:
            msg = redis.blpop(redis_queue)

            payload = msg[1].replace('\'', '\"')

            #post to public titles
            header = {"Content-Type": "application/json"}

            r = requests.post(os.environ.get('TITLES_API_URL'), data=payload, headers=header)
        except Exception, e:
            print e

if __name__ == '__main__':
    process_queue_items()
