'''
Created on Jun 10, 2012

@author: jasonz
'''

from cleaner_base import CleanerBase
import sys
sys.path.append('../')
sys.path.append('../../')
from spider.sources.sohu_star_spider import SohuStarSpider

class SohuStarCleaner(CleanerBase):
    '''
    classdocs
    '''


    def __init__(self, data_adapter_config_path, source_name, clean_try_limit = 3):
        '''
        Constructor
        '''
        CleanerBase.__init__(self, data_adapter_config_path, "sohu_star", "../schema/people.xml")
        get_schema()


    def clean_star_page(self, url_hash, url, features, images):
        
        pass
    
    def clean_star_details(self, url_hash, url, features, images):
        pass
    
    def clean_star_relationship(self, url_hash, url, features, images):
        pass
    
    def clean_star_films(self, url_hash, url, features, images):
        pass

    def clean_star_albums(self, url_hash, url, features, images):
        pass

    def clean(self, url_hash, url, features, images):
        if features['type'] == SohuStarSpider.stage_star_page:
            return self.clean_star_page(url_hash, url, features, images)
        elif features['type'] == SohuStarSpider.stage_star_details:
            return self.clean_star_details(url_hash, url, features, images)
        elif features['type'] == SohuStarSpider.stage_star_films_search:
            return self.clean_star_films(url_hash, url, features, images)
        elif features['type'] == SohuStarSpider.stage_star_music_search:
            return self.clean_star_albums(url_hash, url, features, images)
        elif features['type'] == SohuStarSpider.stage_star_relationship:
            return self.clean_star_relationship(url_hash, url, features, images)
        return False