'''
Created on May 29, 2012

@author: jasonz
'''

from pymongo import Connection
from bs4 import BeautifulSoup

import datetime
import common_utils

class DataCleanStatus:
    NEW_ADDED = 0
    UPDATED = 1
    UPLOADED = 2

class DataCleanAdapter:
    '''
    classdocs
    '''

    def __init__(self, data_adapter_config_path, source_name = None, logger = None):
        f = open(data_adapter_config_path, 'r')
        configs = BeautifulSoup(f.read())
        f.close()
        connection = Connection(configs.connection.host.string, int(configs.connection.port.string))
        self.source_name = source_name

        data_clean_database = connection[configs.data_clean_database.string]
        self.data_clean_coll = data_clean_database[source_name]
        self.data_clean_coll.create_index("url")
        self.data_clean_coll.create_index("url_hash")
        
        self.logger = None
        if logger != None:
            self.logger = logger
            
    
    def get_data_clean_by_url_hash(self, url_hash):
        return self.data_clean_coll.find_one({"url_hash" : url_hash})

    def has_data_clean_by_url_hash(self, url_hash):
        if self.get_data_clean_by_url_hash(url_hash) != None:
            return True
        return False
    
    
    def create_data_clean(self, url_hash, url, features, images = None):
        if not self.has_data_clean_by_url_hash(url_hash): 
            doc = {}
            doc["url_hash"] = url_hash
            doc["url"] = url
            doc["features"] = features
            doc["images"] = images 
            doc["created_at"] = datetime.datetime.now()
            doc["status_flag"] = DataCleanStatus.NEW_ADDED
            self.data_clean_coll.insert(doc)
            return True 
        return False 
    
    def update_data_clean(self, url_hash, features, images = None):
        doc = self.data_raw_coll.find_one({"url_hash" : url_hash})
        if doc != None:
            doc["features"] = features
            doc["images"] = images
            doc["updated_at"] = datetime.datetime.now()
            doc["status_flag"] = DataCleanStatus.UPDATED
            self.data_clean_coll.update({"url_hash" : url_hash}, doc)
            return True
        return False 
