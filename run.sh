export SETTINGS='DEVELOPMENT'
export REDIS_QUEUE_KEY='titles_queue'
export REDIS_URL='localhost'
export PUBLIC_SEARCH_API='http://localhost:8003/load/public_titles'
export AUTHENTICATED_SEARCH_API='http://localhost:8003/load/authenticated_titles'

foreman start
