The Feeder
======

Worker process to consume title json from queue.

** Dependencies **

Redis

** Install Redis **

```
brew install redis
```

** Start Redis **

```
redis-sever
```

** If running locally export the following environment variables **

```
export REDIS_QUEUE_KEY='titles_queue'
export REDIS_HOST='redis://user:@localhost:6379'
```

** Run the application **
```
foreman start
```
