#!/usr/bin/python
# -*- coding:utf-8 -*-
from spider import Spider
import os
import sys

if __name__ == '__main__':
    spider = Spider()
    spider.run()
    python = sys.executable
    os.execl(python, python, *sys.argv)
