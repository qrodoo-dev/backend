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

class MoviesoonNewsSpider(SpiderBase):
   
    first_index_page_update_interval = 10 * 60 
    index_page_update_interval = 86400 * 365 * 30 
    news_page_update_interval = 86400 * 365 * 100
    
    
    def __init__(self, data_adapter_config_path, encode = "utf-8"):
        SpiderBase.__init__(self, data_adapter_config_path, "moviesoon_news", encode)

    def parse_news_node(self, doc, context, created_at):
        features = {}
        image_urls = {} 
        features["title"] = context["title"]
        features["posted_at"] = context["posted_at"]
        features["author"] = context["author"]

        image_count = 0
        images = []
        content_div = doc.find("div", { "class" : "post" })
        if content_div != None:
            features["content"] = []
            first = True
            for p in content_div.findAll("p"):
                if first:
                    first = False
                    continue
                if p.find("span") == None and len(p.text.strip()) > 0:
                    features["content"].append(p.text.strip())
            for a in content_div.findAll("img"):
                url = a.get("src")
                image_count += 1
                images.append({"name" : "img_" + str(image_count), "url" : url, "image_format" : "jpg" })
        
        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.news_page_update_interval) 
          
        return features, images, next_update_time, None

        

    def parse_index_node(self, doc, context):
        page_num = context["page_num"]
           
        children = []
        content_div = doc.find("div", { "id" : "content" })
        for post_div in content_div.findAll("div", { "class" : "post" }):
            try:
                info_div = post_div.find("div", { "class" : "postAyrinti" })
                author = info_div.find("a", { "rel" : "author" }).string
                posted_at = info_div.find("a", { "rel" : "author" }).next.next.string
                title = post_div.h2.a.string
                url = post_div.h2.a.get("href") 
                if not self.url_exists_in_doc_raw(url):
                    context_new = {}
                    context_new["author"] = author 
                    context_new["title"] = title 
                    context_new["posted_at"] = posted_at 
                    children.append({ "url" : url, 
                                      "stage" : "news", 
                                      "context" : context_new, 
                                      "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD }) 
            except BaseException, e:
                self.logger.log("error occur when getting a child page: %s"%(e))

        if len(children) > 0:
            url = "http://moviesoon.com/news/page/" + str(page_num + 1) + "/"
            context_new = { "page_num" : page_num + 1 }
            operation_flag = SpiderChildNodeOperationFlag.NEW_ADD
            if self.url_exists_in_doc_raw(url):
                operation_flag = SpiderChildNodeOperationFlag.FORCE_TO_RECRAWL
            children.append({ "url" : url, 
                              "stage" : "index", 
                              "context" : context_new, 
                              "operation_flag" : operation_flag }) 

        if page_num == 1:
            next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.first_index_page_update_interval) 
        else:
            next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.index_page_update_interval) 

        return None, None, next_update_time, children 

    def parse(self, url_hash, page, encode, stage, context, created_at, page_crawled_at):
        doc = BeautifulSoup(page)
        if stage == "news":
            return self.parse_news_node(doc, context, created_at) 
        elif stage == "index":
            return self.parse_index_node(doc, context)
        return None, None, None, None

