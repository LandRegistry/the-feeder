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

            title_number = json_data.get('title_id', None)
            property_ = json_data.get('property', None)
            address = property_.get('address', None)
            payment = json_data.get('payment', None)

              #build public title data structure
            public_title = {"title_number": title_number,
                  "house_number" : json_data.get('house_number', None),
                  "road" : address.get('road', None),
                  "town" : address.get('town', None),
                  "postcode" : address.get('postcode', None),
                "price_paid": payment.get('price_paid', None)
              }

            title_url = "%s/%s" % (titles_api_url, title_number)
            header = {"Content-Type": "application/json"}
            r = requests.post(title_url,  data=json.dumps(public_title), headers=header)
            print "Posted data to public titles api server: reposnse status code %s'" % r.status_code
        except Exception, e:
            print e

if __name__ == '__main__':
    process_queue_items()
