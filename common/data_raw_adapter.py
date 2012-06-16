# coding=utf-8

import datetime
from bs4 import BeautifulSoup
from pymongo import Connection

class DataRawStatus:
    NEW_ADDED = 0
    UPDATED = 1
    CLEANED = 2
    CLEAN_ERROR = 3
    MAX_NUM_STATUS = 3

class DataRawAdapter:
    
    def __init__(self, data_adapter_config_path, source_name = None, logger = None):
        f = open(data_adapter_config_path, 'r')
        configs = BeautifulSoup(f.read())
        f.close()
        connection = Connection(configs.connection.host.string, int(configs.connection.port.string))
        self.source_name = source_name

        data_raw_database = connection[configs.data_raw_database.string]
        self.parse_retry_limit = int(configs.parse_retry_limit.string)
        self.data_raw_coll = data_raw_database[source_name]
        self.data_raw_coll.create_index("url")
        self.data_raw_coll.create_index("url_hash")
        
        self.logger = None
        if logger != None:
            self.logger = logger

    def get_data_raw_by_url_hash(self, url_hash):
        return self.data_raw_coll.find_one({"url_hash" : url_hash})

    def has_data_raw_by_url_hash(self, url_hash):
        if self.get_data_raw_by_url_hash(url_hash) != None:
            return True
        return False
    
    def create_data_raw(self, url_hash, url, features, images = None):
        if not self.has_data_raw_by_url_hash(url_hash): 
            doc = {}
            doc["url_hash"] = url_hash
            doc["url"] = url
            doc["features"] = features
            doc["images"] = images 
            doc["created_at"] = datetime.datetime.now()
            doc['status_flag'] = DataRawStatus.NEW_ADDED
            self.data_raw_coll.insert(doc)
            return True 
        return False 
    
    def update_data_raw(self, url_hash, features, images = None):
        doc = self.data_raw_coll.find_one({"url_hash" : url_hash})
        if doc != None:
            doc["features"] = features
            doc["images"] = images 
            doc["updated_at"] = datetime.datetime.now()
            doc["status_flag"] = DataRawStatus.UPDATED
            self.data_raw_coll.update({"url_hash" : url_hash}, doc)
            return True
        return False
    
    def update_data_raw_status(self, url_hash, status):
        if type(status) != int:
            return False
        if status > DataRawStatus.MAX_NUM_STATUS:
            return False
        doc = self.data_raw_coll.find_one({"url_hash" : url_hash})
        if doc != None:
            doc["status_flag"] = status
            return True
        return False

    def get_uncleaned_data_raw(self):
        for doc in self.data_raw_coll.find({"status_flag" : DataRawStatus.NEW_ADDED}):
            yield doc['url_hash'], doc['url'], doc['features'], doc['images']
        for doc in self.data_raw_coll.find({"status_flag" : DataRawStatus.UPDATED}):
            yield doc['url_hash'], doc['url'], doc['features'], doc['images']
