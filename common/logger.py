# coding=utf-8
'''
Created on May 8, 2012

@author: jasonz
'''

import datetime
import os
import getpass

class Logger():
    LOG_TO_STD = 0
    LOG_TO_FILE = 1
    LOG_TO_BOTH = 2
    LOG_FILE_PATH = "/home/%user%/log"
    def __init__(self, service_name = "", source_name = ""):
        self.service_name = service_name
        self.source_name = source_name
        print 'service', self.service_name, 'source', self.source_name

    def log_to_file(self, message, path, filename):
        if not path.endswith(os.sep):
            path += os.sep
        if not os.path.isdir(path):
            os.makedirs(path)
        log_file = open(path+filename, "a")
        message = message.encode("utf-8")
        log_file.write(message)

    def log(self, message, logtype=2):
        try:
            meta_info = "[" + datetime.datetime.now().strftime("%Y%m%d%H%M") + "]"
            if self.service_name != "":
                meta_info += "[" + self.service_name + "]"
            if self.source_name != "":
                meta_info += "[" + self.source_name + "]"
            message = meta_info + " " + message
            path = self.LOG_FILE_PATH.replace("%user%", getpass.getuser())
            path = path + os.sep + self.service_name + os.sep + self.source_name
            filename = datetime.datetime.now().strftime("%Y%m%d")
            if logtype == self.LOG_TO_STD:
                print message
            elif logtype == self.LOG_TO_FILE:
                self.log_to_file(message + u'\n', path, filename)
            elif logtype == self.LOG_TO_BOTH:
                print message
                self.log_to_file(message + u'\n', path, filename)
            else:
                print "wrong log type"
        except BaseException, e:
            print("Error occur when logging: %s"%(e))
