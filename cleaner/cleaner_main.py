'''
Created on May 31, 2012

@author: jasonz
'''
from cleaner_threads import CleanerThread
from bs4 import BeautifulSoup
from sources.sohu_star_cleaner import SohuStarCleaner
import sys
sys.path.append('../')
import time

def create_cleaner(data_adapter_config_path, source_name):
    if source_name == "sohu_star":
        return SohuStarCleaner(data_adapter_config_path, source_name)

    return None

def load_cleaners(data_sources_file):
    f = open(data_sources_file, 'r')
    config = BeautifulSoup(f.read())
    f.close()
    
    cleaner_sources = []
    for source in config.sources.findAll("source"):
        if source.cleaner.enable.string == "1":
            cleaner_sources.append(source.source_name.string)
    
    return cleaner_sources 


if __name__ == '__main__':
    database_config_file = "../database_config.xml"
    source_config_file = "../source_config.xml"
    threads = {}

    while True:
        sources = load_cleaners(source_config_file)
        for source_name in sources:
            if not threads.has_key(source_name):
                cleaner = create_cleaner(database_config_file, source_name)
                threads[source_name] = CleanerThread(cleaner)
                threads[source_name].setDaemon(True)
                threads[source_name].start()
        for source_name in threads.keys():
            if not sources.contains(source_name):
                if threads[source_name].is_running:
                    threads[source_name].available = False 
                else:
                    del(threads[source_name])
        time.sleep(30)
