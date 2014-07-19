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
            msg = redis.blpop(redis_queue) #It will wait here until a message is put on the queue
            payload = msg[1].replace('\'', '\"')

            json_data = json.loads(payload)
            title_number = json_data.get('title_number')
            property_ = json_data.get('property')
            address = property_.get('address')
            payment = json_data.get('payment')

            #build public title data structure
            public_title = {"title_number": title_number,
                  "house_number" : address.get('house_number'),
                  "road" : address.get('road'),
                  "town" : address.get('town'),
                  "postcode" : address.get('postcode'),
                  "price_paid": payment.get('price_paid')
            }

            title_url = "%s/%s" % (titles_api_url, title_number)
            header = {"Content-Type": "application/json"}
            r = requests.put(title_url,  data=json.dumps(public_title), headers=header)
            print "Posted data to public titles api server: response status code %s'" % r.status_code
        except Exception, e:
            print e

if __name__ == '__main__':
    process_queue_items()
