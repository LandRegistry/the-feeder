from thefeeder import (
        redis_queue,
        queue_key,
        public_search_api,
        authenticated_search_api,
        historian_api)
from thefeeder.feed_consumer import FeedConsumer
from thefeeder.feed_worker import FeedWorker
from thefeeder.filters import (
        public_filter,
        authenticated_filter,
        history_filter)


if __name__ == '__main__':
    FeedConsumer(
        queue=redis_queue,
        queue_key=queue_key,
        workers=[
            FeedWorker(feed_url=public_search_api, filter=public_filter),
            FeedWorker(
                feed_url=authenticated_search_api,
                filter=authenticated_filter),
            FeedWorker(feed_url=historian_api, filter=history_filter),
        ]
    ).run()
