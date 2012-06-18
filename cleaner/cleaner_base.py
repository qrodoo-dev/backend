'''
Created on Jun 10, 2012

@author: jasonz
'''
from bs4 import BeautifulSoup
from common.data_raw_adapter import DataRawAdapter
from common.data_clean_adapter import DataCleanAdapter
from common.data_raw_adapter import DataRawStatus
from common.logger import Logger

class SchemaField:
    def __init__(self):
        self.name = ""
        self.type = None
        self.required = False

class CleanerBase:
    '''
    classdocs
    '''
    def __init__(self, data_adapter_config_path, source_name, schema_file):
        '''
        Constructord
        '''
        self.logger = Logger("cleaner", source_name)
        self.data_raw_adapter = DataRawAdapter(data_adapter_config_path, source_name, self.logger)
        self.data_clean_adapter = DataCleanAdapter(data_adapter_config_path, source_name, self.logger)
        self.source_name = source_name
        self.get_schema(schema_file)
        
    def get_schema(self, schema_file):
        f = open(schema_file)
        schema = BeautifulSoup(f.read())
        f.close()
        self.required_fields = []
        self.optional_fields = []
        for field in schema.findAll("field"):
            schema_field = SchemaField()
            schema_field.name = field
        
        
        pass

    def clean(self, url_hash, url, features, images):
        pass
        
    def run(self):
        for url_hash, url, features, images in self.data_raw_adapter.get_uncleaned_data_raw():
            success = self.clean(url_hash, url, features, images)
            if not success:
                self.data_raw_adapter.update_data_raw_status(url_hash, DataRawStatus.CLEAN_ERROR)
            else:
                self.data_raw_adapter.update_data_raw_status(url_hash, DataRawStatus.CLEANED)