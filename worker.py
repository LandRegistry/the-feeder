from redis import Redis
import os
import urlparse
import requests
import json

url = urlparse.urlparse(os.environ.get('REDIS_HOST'))
redis_queue = os.environ.get('REDIS_QUEUE_KEY')
redis = Redis(host=url.hostname, port=url.port, password=url.password)
titles_api_url =  os.environ.get('TITLES_API_URL')

def process_queue_items():
    while 1:
        try:
            msg = redis.blpop(redis_queue)
            payload = msg[1].replace('\'', '\"')
            json_data = json.loads(payload)
            title_number = json_data['title_number']
            title_url = "%s/%s" % (titles_api_url, title_number)
            header = {"Content-Type": "application/json"}
            r = requests.post(title_url,  data=payload, headers=header)
            print r.status_code
        except Exception, e:
            print e

if __name__ == '__main__':
    process_queue_items()
