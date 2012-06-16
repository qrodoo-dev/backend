# coding=utf-8
'''
Created on Apr 25, 2012

@author: jasonz
'''
import datetime
import common_utils 
from bs4 import BeautifulSoup
import pymongo
from pymongo import Connection
import os

class ImageIndexStatus :
    NEW_ADDED = 0
    DOWNLOAD_SUCCESS = 1
    DOWNLOAD_FAILED = 2

class ImageStoreAdapter:
    
    def __init__(self, database_config_path, logger = None):
        f = open(database_config_path, 'r')
        configs = BeautifulSoup(f.read())
        f.close()
        
        connection = Connection(configs.connection.host.string, int(configs.connection.port.string))
        image_index_database = connection[configs.image_index_database.string]
        self.image_index_coll = image_index_database["images"]
        self.image_path = configs.image_store_path.string
        
        self.logger = None
        if logger != None:
            self.logger = logger

    def get_image_index_by_image_id(self, image_id):
        return self.image_index_coll.find_one({"image_id" : image_id})

    def has_image_index_by_image_id(self, image_id):
        if self.get_image_index_by_image_id(image_id) != None:
            return True
        return False
    
    def create_image_index(self, image_id, image_format, link = None, path = None ):
        if not self.has_image_index_by_image_id(image_id):
            image_index = {"image_id" : image_id,
                           "image_format" : image_format, 
                           "link" : link,
                           "path" :  path,
                           "created_at" : datetime.datetime.now(),
                           "status_flag" : ImageIndexStatus.NEW_ADDED}
            self.image_index_coll.insert(image_index)
            return True
        return False 
    
    def load_undownloaded_images(self, max_num):
        coll = self.image_index_coll
        for doc in coll.find({"status_flag" : ImageIndexStatus.NEW_ADDED}).limit(max_num):
            try:
                image_id = doc["image_id"]
                link = doc["link"]
                yield image_id, link
            except BaseException, e:
                if self.logger != None:
                    self.logger.log("error while loading undownloaded image: %s......"%(e))
    
    def update_image_status(self, image_id, status_flag):
        doc = self.image_index_coll.find_one({'image_id': image_id})
        doc['status_flag'] = status_flag
        self.image_index_coll.update({'image_id':image_id}, doc)

    def store_image(self, image_id, data):
        path = self.image_path + os.sep + image_id[:2] + os.sep + image_id[2:4]
        try:
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError, e:
            print "Failed at making dir ", path
            print e
            return
        path = path + os.sep + image_id
        image_file = open(path, 'wb')
        image_file.write(data)
        doc = self.image_index_coll.find_one({'image_id': image_id})
        doc['path'] = path
        doc['status_flag'] = ImageIndexStatus.DOWNLOAD_SUCCESS
        self.image_index_coll.update({'image_id': image_id}, doc)
        
