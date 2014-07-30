The Feeder
======
[![Build Status](https://travis-ci.org/LandRegistry/the-feeder.svg)](https://travis-ci.org/LandRegistry/the-feeder)

[![Coverage Status](https://img.shields.io/coveralls/LandRegistry/the-feeder.svg)](https://coveralls.io/r/LandRegistry/the-feeder)

Worker process to consume title json from queue.

####Dependencies

Redis

####Install Redis

```
brew install redis
```

#####Start Redis

```
redis-sever
```

#####This application requires the following environment variables

```
SETTINGS
REDIS_QUEUE_KEY
REDIS_HOST
SETTINGS
REDIS_QUEUE_KEY
REDIS_URL
PUBLIC_SEARCH_API_URL
PUBLIC_SEARCH_API_ENDPOINT
AUTHENTICATED_SEARCH_API_URL
AUTHENTICATED_SEARCH_API_ENDPOINT
```

local development example:

```
export SETTINGS='DEVELOPMENT'
export REDIS_QUEUE_KEY='titles_queue'
export REDIS_URL='localhost'
export PUBLIC_SEARCH_API_URL='http://localhost:8003'
export PUBLIC_SEARCH_API_ENDPOINT='/load/public_titles'
export AUTHENTICATED_SEARCH_API_URL='http://localhost:8003'
export AUTHENTICATED_SEARCH_API_ENDPOINT='/load/authenticated_titles'
export REDIS_QUEUE_KEY='titles_queue'
export REDIS_HOST='redis://user:@localhost:6379'
```


####Run the application
```
foreman start
```
