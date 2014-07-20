export REDIS_QUEUE_KEY='titles_queue'
export REDIS_HOST='redis://user:@localhost:6379'
export PUBLIC_TITLES_API='http://localhost:8005/title'
export PRIVATE_TITLES_API='http://localhost:8008/title'
export SEARCH_API='http://localhost:8003/load'

foreman start
