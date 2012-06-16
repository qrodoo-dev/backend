''' Created on Apr 25, 2012

@author: jasonz
'''
from spider_base import SpiderBase
from spider_thread import SpiderThread
import thread
import time
from bs4 import BeautifulSoup
import sys
sys.path.append('../')

from sources.douban_movie_spider import DoubanMovieSpider
from sources.mtime_news_spider import MtimeNewsSpider 
from sources.sohu_star_spider import SohuStarSpider
from sources.yahoo_news_spider import YahooNewsSpider 
from sources.yisou_all_spider import YisouAllSpider

def create_spider(data_adapter_config_path, source_name, encode):
    if source_name == "douban_movie":
        return DoubanMovieSpider(data_adapter_config_path, encode)
    
    if source_name == "mtime_news":
        return MtimeNewsSpider(data_adapter_config_path, encode)

    if source_name == "sohu_star":
        return SohuStarSpider(data_adapter_config_path, encode)

    if source_name == "yahoo_news":
        return YahooNewsSpider(data_adapter_config_path, encode)
    
    if source_name == "yisou_all":
        return YisouAllSpider(data_adapter_config_path, encode)

    return None

def load_spiders(data_sources_file):
    f = open(data_sources_file, 'r')
    config = BeautifulSoup(f.read())
    f.close()
    
    spider_source_encoding = {} 
    for source in config.sources.findAll("source"):
        if source.spider.enable.string == "1":
            source_name = source.source_name.string
            encoding = source.encoding.string
            spider_source_encoding[source_name] = encoding
    
    return spider_source_encoding 
      
database_config_file = "../database_config.xml" 
source_config_file = "../source_config.xml" 
if __name__ == '__main__':
    threads = {}
    while True:
        spider_source_encoding = load_spiders(source_config_file)
        for source_name, encoding in spider_source_encoding.items():
            if not threads.has_key(source_name):
                spider = create_spider(database_config_file, source_name, encoding)
                threads[source_name] = SpiderThread(spider)
                threads[source_name].setDaemon(True)
                threads[source_name].start()
        for source_name in threads.keys():
            if not spider_source_encoding.has_key(source_name):
                if threads[source_name].is_running:
                    threads[source_name].available = False 
                else:
                    del(threads[source_name])

        time.sleep(30)
                

