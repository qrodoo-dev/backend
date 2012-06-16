# coding=utf-8
'''
Created on Apr 25, 2012

@author: jasonz
'''

import threading
import sys
import time 

class SpiderThread(threading.Thread):

    def __init__(self, spider, exploring_interval_time = 10):
        threading.Thread.__init__(self)
        self.spider = spider
        self.available = True
        self.is_running = False
        self.exploring_interval_time = exploring_interval_time 
    
    def run(self):
        self.is_running = True
        while self.available:
            self.spider.spider_run()
            time.sleep(self.exploring_interval_time)
        self.is_running = False 
