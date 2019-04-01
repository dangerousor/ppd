#!/usr/bin/python
# -*- coding:utf-8 -*-
import time

import requests
import json

from rd import r_ip


class Ip:

    @staticmethod
    def get_ips():
        res = requests.get(url='http://d.jghttp.golangapi.com/getip?num=5&type=2&pro=150000&city=150100&yys=0&port=11&time=1&ts=1&ys=0&cs=0&lb=1&sb=0&pb=45&mr=2&regions=', timeout=30)
        if res.status_code != 200:
            print(res.content.decode())
            exit(res.status_code)
        result = json.loads(res.content.decode())
        if result['code'] != 0:
            print(result['msg'])
            exit(result['code'])
        for each in result['data']:
            expire_time = time.mktime(time.strptime(each['expire_time'], '%Y-%m-%d %H:%M:%S')) - time.time()
            if expire_time <= 0:
                continue
            r_ip.set(each['ip'], each['port'])
            r_ip.expire(each['ip'], int(expire_time/100)*100)
