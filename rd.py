#!/usr/bin/python
# -*- coding:utf-8 -*-
import redis

host = 'r-hp3ee2d77296a894.redis.huhehaote.rds.aliyuncs.com'
# host = '127.0.0.1'
port = 6379
r = redis.StrictRedis(host=host, port=port, db=3)
r_ip = redis.StrictRedis(host=host, port=port, db=2)
