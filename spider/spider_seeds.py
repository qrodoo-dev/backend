# coding=utf-8

import datetime 
import sys
import os 
from bs4 import BeautifulSoup
sys.path.append('../')
from common.doc_raw_adapter import DocRawAdapter 
from common import common_utils 

def print_resource():
    print("spider_seeds.py source_name_1, [source_name_2, ...]")

database_config_path = "../database_config.xml"
if __name__ == '__main__':
    
    for i in range(1, len(sys.argv)):
        source_name = sys.argv[i]
        seed_path = "seeds/" + source_name + ".xml"
        if os.path.exists(seed_path):
            print("Loading seed from file %s..."%(seed_path))
            f = open(seed_path, "r")
            seeds = BeautifulSoup(f.read())
            f.close()

            doc_raw_adapter = DocRawAdapter(database_config_path, source_name)
            for seed in seeds.findAll("seed"):
                url = seed.url.string
                stage = seed.stage.string
                context = {}
                for content in seed.context.findAll():
                    content_type = content.get("type")
                    if content_type == "int":
                        context[content.name] = int(content.string)
                    else:
                        context[content.name] = content.string
                
                url_hash = common_utils.gen_url_hash(url)
                if not doc_raw_adapter.has_doc_raw_by_url_hash(url_hash):
                    doc_raw_adapter.create_doc_raw(url_hash, url, stage, context)
                    print("%s added into %s"%(url, source_name))
        else:
            print("Can't find seed file %s"%(seed_path))




