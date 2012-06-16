# coding=utf-8

import datetime
import common_utils 
from bs4 import BeautifulSoup
import pymongo
from pymongo import Connection
import os


class DocRawStatus:
    NEW_ADDED = 0
    PAGE_CRAWLED = 1
    ERROR_FAILED_TO_CRAWLED = 2
    DATA_PARSED = 3
    ERROR_FAILED_TO_PARSED = 4
    CRAWL_FAILED = 5

class DocRawAdapter:
    MAX_CRAWL_FAIL_TIMES = 3
    
    def __init__(self, database_config_path, source_name, logger = None):
        f = open(database_config_path, 'r')
        configs = BeautifulSoup(f.read())
        f.close()
        connection = Connection(configs.connection.host.string, int(configs.connection.port.string))
        self.source_name = source_name
        doc_raw_database = connection[configs.doc_raw_database.string]
        self.doc_raw_coll = doc_raw_database[source_name]
        self.doc_raw_coll.create_index("url")
        self.doc_raw_coll.create_index("url_hash")
        self.doc_raw_coll.create_index("stage")
        self.doc_raw_coll.create_index("status_flag")
        self.doc_raw_coll.create_index("next_update_time")
        self.doc_raw_coll.create_index("crawl_priority")
        
        self.logger = None
        if logger != None:
            self.logger = logger

    def get_doc_raw_by_url_hash(self, url_hash):
        return self.doc_raw_coll.find_one({"url_hash" : url_hash})

    def has_doc_raw_by_url_hash(self, url_hash):
        if self.get_doc_raw_by_url_hash(url_hash) != None:
            return True
        return False
    
    def create_doc_raw(self, url_hash, url, stage, context = None, father = None):
        if not self.has_doc_raw_by_url_hash(url_hash): 
            self.doc_raw_coll.insert({"url_hash" : url_hash, 
                                      "url" : url,
                                      "stage" : stage,
                                      "context" : context,
                                      "father" : father,
                                      "created_at": datetime.datetime.now(),
                                      "status_flag" : DocRawStatus.NEW_ADDED
                                      })
            return True 
        return False 
    
    def update_doc_raw_with_crawled_page(self, url_hash, encode, page):
        doc = self.get_doc_raw_by_url_hash(url_hash) 
        if doc != None:
            compressed_page = common_utils.compress(page) 
            compressed_page = unicode(compressed_page, "latin-1")
            doc["page"] = compressed_page 
            doc["encode"] = encode
            doc["status_flag"] = DocRawStatus.PAGE_CRAWLED
            doc["page_crawled_at"] = datetime.datetime.now()
            if doc.has_key("next_update_time"):
                del doc["next_update_time"]
            self.doc_raw_coll.update({"url_hash" : url_hash}, doc)
            return True
        return False

    def update_doc_raw_with_node_info(self, url_hash, next_update_time = None, stage = None, context = None, father = None, children = None, parse_try_times = None, status_flag = None):
        doc = self.get_doc_raw_by_url_hash(url_hash) 
        if doc != None:
            if children != None:
                doc["children"] = children 
            if next_update_time != None:
                doc["next_update_time"] = next_update_time 
            if stage != None:
                doc["stage"] = stage 
            if context != None:
                doc["context"] = context 
            if father != None:
                doc["father"] = father 
            if children != None:
                doc["children"] = children 
            if parse_try_times != None:
                doc["parse_try_times"] = parse_try_times 
            if status_flag != None:
                doc["status_flag"] = status_flag 
            doc["updated_at"] = datetime.datetime.now()
            self.doc_raw_coll.update({"url_hash" : url_hash}, doc)
            return True
        return False

    def update_doc_raw_as_crawled_failed(self, url_hash):
        doc = self.doc_raw_coll.find_one({"url_hash" : url_hash})
        if doc == None:
            return
        if doc.has_key("crawl_fail_times"):
            doc['crawl_fail_times'] = doc['crawl_fail_times'] + 1
        else:
            doc['crawl_fail_times'] = 1
        doc['crawl_priority'] = 3
        if doc['crawl_fail_times'] > self.MAX_CRAWL_FAIL_TIMES:
            doc["status_flag"] = DocRawStatus.CRAWL_FAILED
            doc["page_crawled_at"] = datetime.datetime.now()
            self.doc_raw_coll.update({"url_hash" : url_hash}, doc)
            return True
        return False

    def load_uncrawled_docs(self, max_num):
        yield_count = 0
        
        for doc in self.doc_raw_coll.find({"status_flag" : DocRawStatus.NEW_ADDED})\
                .sort("crawl_priority", pymongo.ASCENDING):
            try:
                yield doc["url_hash"], doc["url"]
                yield_count += 1
                if yield_count >= max_num:
                    break
            except BaseException, e:
                if self.logger != None:
                    self.logger.log("error while finding new added doc_raw : %s......"%(e))
        
        for doc in self.doc_raw_coll.find({"status_flag" : DocRawStatus.DATA_PARSED})\
                .sort("next_update_time", pymongo.ASCENDING).sort('crawl_priority', pymongo.ASCENDING):
            try:
                if doc.has_key("next_update_time"):
                    if ((datetime.datetime.now() - doc["next_update_time"]).total_seconds() >= 0):
                        yield doc["url_hash"], doc["url"] 
                        yield_count += 1
                        if yield_count >= max_num:
                            break
                    else:
                        break
            except BaseException, e:
                if self.logger != None:
                    self.logger.log("error while finding doc_raws for update: %s......"%(e))

    def load_unparsed_doc_raw(self):
        for doc in self.doc_raw_coll.find({"status_flag" : DocRawStatus.PAGE_CRAWLED}):
            try:
                stage = doc["stage"]
                page = doc["page"]
                page = page.encode("latin-1")
                page = common_utils.uncompress(page)
                encode = doc["encode"]
                context = None
                if doc.has_key("context"):
                    context = doc["context"]
                created_at = doc["created_at"]
                page_crawled_at = doc["page_crawled_at"]
                yield doc["url_hash"], doc["url"], stage, page, encode, context, created_at, page_crawled_at 
            except BaseException, e:
                if self.logger != None:
                    self.logger.log("error while loading unparsed doc_raw: %s......"%(e))
    
    def get_doc_raw_parse_try_times(self, url_hash):
        doc = self.get_doc_raw_by_url_hash(url_hash)
        if doc == None:
            return None
        else:
            if doc.has_key("parse_try_times"):
                return doc["parse_try_times"]
            else:
                return 0

