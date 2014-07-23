export SETTINGS='DEVELOPMENT'
export REDIS_QUEUE_KEY='titles_queue'
export REDIS_URL='redis://rediscloud:aD45jy220LqB3jBf@pub-redis-17503.eu-west-1-1.1.ec2.garantiadata.com:17503'
export PUBLIC_SEARCH_API='http://localhost:8003/load/public_titles'
export AUTHENTICATED_SEARCH_API='http://localhost:8003/load/authenticated_titles'

foreman start
