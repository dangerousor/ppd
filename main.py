#!/usr/bin/python
# -*- coding:utf-8 -*-
from spider import Spider
import os
import sys

if __name__ == '__main__':
    spider = Spider()
    spider.run()
    os.execv(__file__, sys.argv)
