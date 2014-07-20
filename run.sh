export REDIS_QUEUE_KEY='titles_queue'
export REDIS_HOST='redis://user:@localhost:6379'
export PUBLIC_TITLES_API_URL='http://localhost:8005/title'
export PRIVATE_TITLES_API_URL='http://localhost:8008/title'

foreman start
