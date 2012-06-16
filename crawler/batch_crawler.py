'''
Created on May 8, 2012

@author: jasonz
'''
import time
import datetime
import sys
sys.path.append('../')
from common.doc_raw_adapter import DocRawAdapter
from common.logger import Logger
from common import common_utils 
class BatchCrawler():
    
    MAX_DOCS_NUM = 100
    
    def __init__(self, database_config_path, source_name, domain, encode, request_interval):
        self.logger = Logger("crawler", domain)
        self.adapter = DocRawAdapter(database_config_path, source_name, self.logger)
        self.domain = domain
        self.encode = encode 
        self.request_interval = request_interval
    
    def run(self):
        while True:
            count = 0
            try:
                for url_hash, url in self.adapter.load_uncrawled_docs(BatchCrawler.MAX_DOCS_NUM):
                    count += 1
                    self.logger.log("crawling url %s"%url, 2)
                    page = common_utils.page_crawl(url)
                    if page == None:
                        self.adapter.update_doc_raw_as_crawled_failed(url_hash)
                        continue
                    if self.encode != "utf-8":
                        page = unicode(page, self.encode).encode("utf-8")

                    self.adapter.update_doc_raw_with_crawled_page(url_hash, "utf-8", page)
                    time.sleep(float(self.request_interval))
                if count < BatchCrawler.MAX_DOCS_NUM:
                    break
            except:
                self.logger.log("mongo error")
