'''
Created on May 7, 2012

@author: jasonz
'''
from batch_crawler import BatchCrawler
from bs4 import BeautifulSoup
from multiprocessing import Pool
import time
import xml.dom.minidom

class CrawlerData():
    def __init__(self):
        self.data_adapter_config_path = ""
        self.source_name = ""
        self.update_interval_seconds = -1
        self.encode = 'utf-8'
        self.domain = ""
    
    def __str__(self):
        return self.data_adapter_config_path + " " + self.source_name + " " + self.encode + " " + str(self.update_interval_seconds) + " " +  self.domain + " " +str(self.request_interval_seconds)
    

def create_crawler(crawler_data):
    crawler = BatchCrawler(crawler_data.database_config_file,
                           crawler_data.source_name,
                           crawler_data.domain,
                           crawler_data.encode,
                           crawler_data.request_interval_seconds)
    crawler.run()
    return 1

def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)

def load_crawlers(database_config_file, data_sources_file):
    pool = Pool(processes=10)
    while True:
        f = open(data_sources_file, 'r')
        xml_string = f.read()
        config = BeautifulSoup(xml_string, "xml")
        f.close()
        crawlers = []
        #print config
        dom = xml.dom.minidom.parseString(xml_string)
        sources = dom.getElementsByTagName("source")
        for source in sources:
            crawler = source.getElementsByTagName("crawler")[0]
            if getText(crawler.getElementsByTagName("enable")[0].childNodes) == '1':
                crawler_data = CrawlerData()
                crawler_data.domain = getText(source.getElementsByTagName("domain")[0].childNodes)
                crawler_data.encode = getText(source.getElementsByTagName("encoding")[0].childNodes)
                crawler_data.source_name = getText(source.getElementsByTagName("source_name")[0].childNodes)
                crawler_data.database_config_file = database_config_file 
                crawler_data.request_interval_seconds = int(getText(crawler.getElementsByTagName("request_interval_seconds")[0].childNodes))
                crawlers.append(crawler_data)
        for c in crawlers:
            print c
        pool.map_async(create_crawler, crawlers).get(999999)
        time.sleep(20)
        
    pool.close()
    pool.join()

if __name__ == '__main__':
    crawlers = load_crawlers("../database_config.xml", "../source_config.xml")
    
    
