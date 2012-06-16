# coding=utf-8
'''
Created on Apr 25, 2012

@author: jasonz
'''
import time
import datetime 
import urllib
from spider_base import SpiderBase
from spider_base import SpiderChildNodeOperationFlag 
import re
from bs4 import BeautifulSoup

class DoubanMovieSpider(SpiderBase):
   
    stop_by_empty_count = 3
    tag_list_page_update_interval = 86400 * 365 
    index_page_update_interval = 86400 * 14 
    entity_page_update_interval = 86400 * 365 * 10
    
    
    def __init__(self, data_adapter_config_path, encode = "utf-8"):
        SpiderBase.__init__(self, data_adapter_config_path, "douban_movie", encode)

    def parse_entity_node(self, doc, context):
        features = {}
        features["titles"] = []
        for title in context["titles"].split("/"):
            features["titles"].append(title.strip())
            
        info_div = doc.find("div", { "id" : "info" })
        text = info_div.text
        for line in text.split("\n"):
            tokens = line.split(":")
            if len(tokens) >= 2:
                key = tokens[0]
                features[key] = []
                for value in tokens[1].split("/"):
                    features[key].append(value.strip())
       
        mainpic_dic = doc.find("div", {"id" : "mainpic"})
        url = mainpic_dic.img.get("src")
        url = url.replace("mpic", "lpic")
        images = [ {"name" : "poster", "url" : url, "image_format" : "jpg" } ]
    
        related_info_div = doc.find("div", { "class" : "related_info" })
        if related_info_div != None:
            description_span = related_info_div.find("span", { "property" : "v:summary" })
            if description_span != None:
                description = description_span.text
                features["description"] = description
        
        tags = []
        if context.has_key("tag"):
            tags.append(context["tag"])
        tags_div = doc.find("div", { "id" : "db-tags-section" })
        if tags_div != None:
            for a in tags_div.findAll("a"):
                tags.append(a.string)
        if len(tags) != None:
            features["tags"] = tags

        recommend_div = doc.find("div", { "id" : "db-rec-section" })
        if recommend_div != None:
            recommend_movies = []
            for dl in recommend_div.findAll("dl"):
                a = dl.a
                if a != None:
                    recommend_movies.append(a.get("href")) 
            if (len(recommend_movies) > 0):
                features["recommend_movies"] = recommend_movies

        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.entity_page_update_interval) 
       
        return features, images, next_update_time, None
   
    def parse_tag_list_node(self, doc):
        children = []
        
        content_div = doc.find("div", { "id" : "content" })
        for table in content_div.findAll("table"):
            for tag_a in table.findAll("a"):
                tag = unicode(tag_a.string)
                url = "http://movie.douban.com/tag/" + urllib.quote(tag.encode("utf-8"))
                
                if not self.url_exists_in_doc_raw(url):
                    context_new = { "tag" : tag }
                    children.append({ "url" : url, 
                                      "stage" : "index", 
                                      "context" : context_new, 
                                      "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD })
        
        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.tag_list_page_update_interval) 
        
        return None, None, next_update_time, children 
    
    def parse_index_node(self, doc, context):
        
        children = []
        tag = context["tag"]
        subject_list = doc.find("div", { "id" : "subject_list" })
        for table in subject_list.findAll("table"):
            try:
                url = table.a.get("href")
                titles = table.find("div", { "class" : "pl2" }).a.text
                context_new = { "titles" : titles, "tag" : tag }
                operation_flag = SpiderChildNodeOperationFlag.NEW_ADD
                if self.url_exists_in_doc_raw(url):
                    operation_flag = SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
                children.append({ "url" : url, 
                                  "stage" : "entity", 
                                  "context" : context_new, 
                                  "operation_flag" : operation_flag })
            except BaseException, e:
                self.logger.log("error occur: %s"%(e))


        next_page_span = subject_list.find("span", { "class" : "next" })
        if next_page_span != None:
            try:
                a = next_page_span.a
                if a != None:
                    url = a.get("href")
                    if not self.url_exists_in_doc_raw(url):
                        context_new = { "tag" : tag }
                        children.append({ "url" : url, 
                                          "stage" : "index", 
                                          "context" : context_new, 
                                          "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD })
            except BaseException, e:
                self.logger.log("error occur: %s"%(e))
        
        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.index_page_update_interval) 
        
        return None, None, next_update_time, children 

    def parse(self, url_hash, page, encode, stage, context, created_at, page_crawled_at):
        doc = BeautifulSoup(page)
        if stage == "tag_list":
            return self.parse_tag_list_node(doc) 
        elif stage == "entity":
            return self.parse_entity_node(doc, context) 
        elif stage == "index":
            return self.parse_index_node(doc, context)
        return None, None, None, None

