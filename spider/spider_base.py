# coding=utf-8

import urllib2
import md5
import time
import datetime
import sys
sys.path.append('../')
from common import common_utils 
from common.doc_raw_adapter import DocRawStatus
from common.doc_raw_adapter import DocRawAdapter 
from common.data_raw_adapter import DataRawAdapter 
from common.image_store_adapter import ImageStoreAdapter 
from common.logger import Logger

class SpiderChildNodeOperationFlag:
    NEW_ADD = 0 
    UPDATE_INFO_ONLY = 1 
    FORCE_TO_REPARSE = 2
    FORCE_TO_RECRAWL = 3

class SpiderBase():

    def __init__(self, data_adapter_config_path, source_name, encode = "utf-8", parse_try_limit = 3):
        self.logger = Logger("spider", source_name)  
        
        self.doc_raw_adapter = DocRawAdapter(data_adapter_config_path, source_name, self.logger)
        self.data_raw_adapter = DataRawAdapter(data_adapter_config_path, source_name, self.logger)
        self.image_store_adapter = ImageStoreAdapter(data_adapter_config_path, self.logger)
        self.source_name = source_name
        self.encode = encode 
        self.parse_try_limit = parse_try_limit
        self.exploring_times = 0
    
    
    def url_exists_in_doc_raw(self, url):
        url_hash = common_utils.gen_url_hash(url)
        return self.doc_raw_adapter.has_doc_raw_by_url_hash(url_hash)
        
    def url_hash_exists_in_data_raw(self, url_hash):
        return self.data_raw_adapter.has_data_raw_by_url_hash(url_hash)

    def parse(self, url_hash, page, encode, stage, context, created_at, page_crawled_at):
        '''
        you must override this function
        '''
        self.logger.log("what the hell!!!you have to override to implement parse logic!!!")
        
        features = {} 
        
        images = []
        images.append({"name" : "test_image_name", "url" : "test_image_url", "image_format" : "jpg"})
        
        next_update_time = None
        
        children = []
        children.append({"url" : "test_url", "stage" : "test_stage", "context" : "test_context", "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD}) 

        return features, images, next_update_time, children 

    def explore_child(self, father_url_hash, url, url_hash, stage, context, operation_flag):
        if operation_flag == SpiderChildNodeOperationFlag.NEW_ADD:
            if not self.doc_raw_adapter.has_doc_raw_by_url_hash(url_hash):
                self.doc_raw_adapter.create_doc_raw(url_hash, url, stage, context, father_url_hash)
                self.logger.log("child [%s] %s new added."%(url_hash, url))

        else:
            if self.doc_raw_adapter.has_doc_raw_by_url_hash(url_hash):
                if operation_flag == SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY:
                    self.doc_raw_adapter.update_doc_raw_with_node_info(url_hash, 
                                                                       stage = stage, 
                                                                       context = context, 
                                                                       father = father_url_hash) 
                    self.logger.log("child [%s]'s info is updated."%(url_hash))
                elif operation_flag == SpiderChildNodeOperationFlag.FORCE_TO_REPARSE:
                    self.doc_raw_adapter.update_doc_raw_with_node_info(url_hash, 
                                                                       stage = stage, 
                                                                       context = context, 
                                                                       father = father_url_hash, 
                                                                       status_flag = DocRawStatus.PAGE_CRAWLED)
                    self.logger.log("child [%s] is set to reparse data."%(url_hash))
                elif operation_flag == SpiderChildNodeOperationFlag.FORCE_TO_RECRAWL:
                    self.doc_raw_adapter.update_doc_raw_with_node_info(url_hash, 
                                                                       stage = stage, 
                                                                       context = context, 
                                                                       father = father_url_hash, 
                                                                       status_flag = DocRawStatus.NEW_ADDED)
                    self.logger.log("child [%s]'s is set to recrawled page."%(url_hash))
                

    def spider_run(self):
        for url_hash, url, stage, page, encode, context, created_at, page_crawled_at in self.doc_raw_adapter.load_unparsed_doc_raw():
            try:
                self.logger.log("parsing [%s]."%(url_hash))
                features, images, next_update_time, children = self.parse(url_hash, page, encode, stage, context, created_at, page_crawled_at)
                if images != None:
                    for i in range(0, len(images)):
                        try:
                            image_id = common_utils.gen_url_hash(images[i]["url"])
                            if not self.image_store_adapter.has_image_index_by_image_id(image_id):
                                images[i]["image_id"] = image_id
                                self.image_store_adapter.create_image_index(image_id, images[i]["image_format"], images[i]["url"])
                                self.logger.log("image [%s] created for [%s]."%(image_id, url_hash))
                        except BaseException, e:
                            self.logger.log("Error occured when creating image index: %s"%(e))
                
                if features != None:
                    if not self.url_hash_exists_in_data_raw(url_hash):
                        self.data_raw_adapter.create_data_raw(url_hash, url, features, images)
                        self.logger.log("features for [%s] is added."%(url_hash))
                    else:
                        self.data_raw_adapter.update_data_raw(url_hash, features, images)
                        self.logger.log("features for [%s] is updated."%(url_hash))

                children_url_hashes = None 
                if children != None:
                    children_url_hashes = []
                    for child in children:
                        try:
                            url_new = child["url"]
                            url_hash_new = common_utils.gen_url_hash(child["url"])
                            stage_new = child["stage"]
                            context_new = child["context"]
                            operation_flag = child["operation_flag"]
                            
                            self.explore_child(url_hash, url_new, url_hash_new, stage_new, context_new, operation_flag)
                            
                            children_url_hashes.append(url_hash_new)
                        except BaseException, e:
                            self.logger.log("Error occured when exploring child: %s"%(e))
                
                self.doc_raw_adapter.update_doc_raw_with_node_info(url_hash, 
                                                                   next_update_time = next_update_time, 
                                                                   children = children_url_hashes,
                                                                   status_flag = DocRawStatus.DATA_PARSED)
             
            except BaseException, e:
                self.logger.log("Error occured in main spider_run: %s"%(e))
                if url_hash != None:
                    parse_try_times = self.doc_raw_adapter.get_doc_raw_parse_try_times(url_hash)
                    if parse_try_times + 1 >= self.parse_try_limit:
                        self.doc_raw_adapter.update_doc_raw_with_node_info(url_hash, 
                                                                           status_flag = DocRawStatus.ERROR_FAILED_TO_PARSED)
                    else:
                        self.doc_raw_adapter.update_doc_raw_with_node_info(url_hash, 
                                                                           next_update_time = datetime.datetime.now() + datetime.timedelta(86400),
                                                                           parse_try_times = parse_try_times + 1, 
                                                                           status_flag = DocRawStatus.NEW_ADDED)

