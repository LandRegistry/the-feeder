from thefeeder import redis_queue, queue_key, public_search_api, authenticated_search_api
from thefeeder.feed_consumer import Consumer
from thefeeder.feed_worker import Worker
from thefeeder.filters import public_filter, authenticated_filter


if __name__ == '__main__':
    Consumer(
        queue=redis_queue,
        queue_key=queue_key,
        workers=[
            Worker(feed_url=public_search_api, filter=public_filter),
            Worker(feed_url=authenticated_search_api, filter=authenticated_filter),
        ]
    ).run()
