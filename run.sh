export SETTINGS='DEVELOPMENT'
export REDIS_QUEUE_KEY='titles_queue'
export REDIS_URL='localhost'
export PUBLIC_SEARCH_API_URL='http://localhost:8003'
export PUBLIC_SEARCH_API_ENDPOINT='/load/public_titles'
export AUTHENTICATED_SEARCH_API_URL='http://localhost:8003'
export AUTHENTICATED_SEARCH_API_ENDPOINT='/load/authenticated_titles'
export GEO_API_URL='http://localhost:8008'
export GEO_API_ENDPOINT='/titles/<title_number>'

foreman start
