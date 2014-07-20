export REDIS_QUEUE_KEY='titles_queue'
export REDIS_HOST='redis://user:@localhost:6379'
export PUBLIC_SEARCH_API='http://localhost:8003/load/public_titles'
export AUTHENTICATED_SEARCH_API='http://localhost:8003/load/authenticated_titles'

foreman start
