#!/usr/bin/python
# -*- coding:utf-8 -*-
import redis
import random

host = 'r-hp3ee2d77296a894.redis.huhehaote.rds.aliyuncs.com'
port = 6379
r = redis.StrictRedis(host=host, port=port, db=3)

i = 10000000
while i < 20000000:
        r.sadd('yet', str(i))
        i += random.randint(1, 10)
