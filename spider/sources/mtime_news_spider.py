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

class MtimeNewsSpider(SpiderBase):
   
    first_index_page_update_interval = 10 * 60 
    index_page_update_interval = 86400 * 365 * 30 
    hot_news_page_update_interval = 60 * 30 
    other_news_page_update_interval = 86400 * 365 * 100
    
    
    def __init__(self, data_adapter_config_path, encode = "utf-8"):
        SpiderBase.__init__(self, data_adapter_config_path, "mtime_news", encode)

    def parse_news_node(self, url_hash, doc, context, created_at):
        features = {}
        features["title"] = context["title"]
        features["posted_at"] = context["posted_at"]
        features["category"] = context["category"]
        features["group"] = context["group"]
        features["is_start_page"] = context["is_start_page"] 
        if context.has_key("start_page_url_hash"):
            features["start_page_url_hash"] = context["start_page_url_hash"] 
        features["page_num"] = context["page_num"] 
        
        images = [] 
        image_count = 0
        image_region = doc.find("img", { "id" : "imageRegion" })
        if image_region != None:
            url = image_region.get("src")
            image_format = "jpg"
            if url.endswith(".png"):
                image_format = "png"
            images.append({"name" : "img_" + str(image_count), "url" : url, "image_format" : image_format })
            image_count += 1
        
        newscont_div = doc.find("div", { "id" : "newscont" })
        if newscont_div == None: 
            newscont_div = doc.find("div", { "class" : "news_conters" })
        if newscont_div == None: 
            newscont_div = doc.find("div", { "class" : "text px14 lh16" })
        if newscont_div != None:
            features["content"] = []
            for p in newscont_div.findAll("p"):
                if p.get("align") == "center" and p.find("img") != None:
                    url = p.img.get("src")
                    image_format = "jpg"
                    if url.endswith(".png"):
                        image_format = "png"
                    images.append({"name" : "img_" + str(image_count), "url" : url, "image_format" : image_format })
                    image_count += 1
                if p.get("align") == None and p.get("class") == None:
                    paragraph = p.text.strip()
                    if len(paragraph) > 5: 
                        features["content"].append(paragraph)

        comment_count_span = doc.find("span", { "id" : "span_comment_count2" })
        if comment_count_span != None:
            features["comment_count"] = int(comment_count_span.text)

        features["is_end_page"] = 1
        children = []
        page_nav_div = doc.find("div", { "class" : "pagenav tc" })
        if page_nav_div != None:
            for next_div in page_nav_div.findAll("a", { "class" : "ml10 next" }):
                if next_div.text == u"下一页":
                    url = next_div.get("href")
    
                    features["is_end_page"] = 0
                    if not self.url_exists_in_doc_raw(url):
                        context_new = {}
                        context_new["title"] = features["title"] 
                        context_new["posted_at"] = features["posted_at"] 
                        context_new["category"] = features["category"] 
                        context_new["group"] = features["group"] 
                        context_new["is_start_page"] = 0 
                        if features.has_key("start_page_url_hash"):
                            context_new["start_page_url_hash"] = features["start_page_url_hash"] 
                        else:
                            context_new["start_page_url_hash"] = url_hash
                        context_new["page_num"] = features["page_num"] + 1 
            
                        children.append({ "url" : url, 
                                          "stage" : "news", 
                                          "context" : context_new, 
                                          "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD }) 
                    break

                
            
        
        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.other_news_page_update_interval) 
        if ((datetime.datetime.now() - created_at).total_seconds() <=  86400 * 3):
            next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.hot_news_page_update_interval) 
            
        return features, images, next_update_time, children


    def parse_index_node(self, doc, context):
        category = context["category"]
        group = context["group"]
        page_num = context["page_num"]
        url_prefix = context["url_prefix"]
           
        children = []
        for news_list in doc.findAll("ul", { "class" : "news_lists" }):
            for li in news_list.findAll("li"):
                url = li.a.get("href")
                if not self.url_exists_in_doc_raw(url):
                    context_new = {}
                    context_new["title"] = li.a.string 
                    context_new["posted_at"] = li.span.string 
                    context_new["category"] = category
                    context_new["group"] = group 
                    context_new["is_start_page"] = 1 
                    context_new["page_num"] = 1 
        
                    children.append({ "url" : url, 
                                      "stage" : "news", 
                                      "context" : context_new, 
                                      "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD }) 
        
        if len(children) > 0:
            url = url_prefix + "index-" + str(page_num + 1) + ".html"
            context_new = {}
            context_new["category"] = category
            context_new["group"] = group 
            context_new["page_num"] = page_num + 1
            context_new["url_prefix"] = url_prefix 
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
            return self.parse_news_node(url_hash, doc, context, created_at) 
        elif stage == "index":
            return self.parse_index_node(doc, context)
        return None, None, None, None

