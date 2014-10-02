import os
import time
import urlparse
import logging
import sys

from redis import Redis
from thefeeder.public_message_datatype import PublicMessageDatatype

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

config_name = os.environ.get('SETTINGS')
if not config_name:
    logger.error("You must set the SETTINGS  environment variable")
    sys.exit(-1)

redis_url = os.environ.get('REDIS_URL')
queue_key = os.environ.get('REDIS_QUEUE_KEY')
public_search_api = os.environ.get('PUBLIC_SEARCH_API_URL', '') + os.environ.get('PUBLIC_SEARCH_API_ENDPOINT', '')
authenticated_search_api = os.environ.get('AUTHENTICATED_SEARCH_API_URL', '') + os.environ.get('AUTHENTICATED_SEARCH_API_ENDPOINT', '')
geo_api = os.environ.get('GEO_API_URL', '') + os.environ.get('GEO_API_ENDPOINT', '')
redis_queue = None
public_message_validator = PublicMessageDatatype()

if config_name.upper() == 'PRODUCTION':
    while True:
        try:
            url = urlparse.urlparse(redis_url)
            logger.info("Try to connect to Redis on %s %s" % (url.hostname, url.port))
            redis_queue = Redis(host=url.hostname, port=url.port, password=url.password)
            break
        except RuntimeError as e:
            logger.error("Failed to connect to Redis %e" % e)

        time.sleep(10)

else:
    redis_queue = Redis(redis_url)
