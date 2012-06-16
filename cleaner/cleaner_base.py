'''
Created on Jun 10, 2012

@author: jasonz
'''

from common.data_raw_adapter import DataRawAdapter
from common.data_clean_adapter import DataCleanAdapter
from common.data_raw_adapter import DataRawStatus
from common.logger import Logger

class CleanerBase:
    '''
    classdocs
    '''
    def __init__(self, data_adapter_config_path, source_name, clean_try_limit=3):
        '''
        Constructord
        '''
        self.logger = Logger("cleaner", source_name)
        self.data_raw_adapter = DataRawAdapter(data_adapter_config_path, source_name, self.logger)
        self.data_clean_adapter = DataCleanAdapter(data_adapter_config_path, source_name, self.logger)
        self.source_name = source_name
        self.clean_try_limit = clean_try_limit

    def clean(self, url_hash, url, features, images):
        pass
        
    def run(self):
        for url_hash, url, features, images in self.data_raw_adapter.get_uncleaned_data_raw():
            success = self.clean(url_hash, url, features, images)
            if not success:
                self.data_raw_adapter.update_data_raw_status(url_hash, DataRawStatus.CLEAN_ERROR)
            else:
                self.data_raw_adapter.update_data_raw_status(url_hash, DataRawStatus.CLEANED)