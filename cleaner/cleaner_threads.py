# coding=utf-8
'''
Created on Apr 25, 2012

@author: jasonz
'''

import threading
import sys
import time 

class CleanerThread(threading.Thread):

    def __init__(self, cleaner, interval_time = 10):
        threading.Thread.__init__(self)
        self.cleaner = cleaner
        self.available = True
        self.is_running = False
        self.interval_time = interval_time 
    
    def run(self):
        self.is_running = True
        while self.available:
            self.cleaner.run()
            time.sleep(self.interval_time)
        self.is_running = False 
