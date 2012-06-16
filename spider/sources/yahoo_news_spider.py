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

class YahooNewsSpider(SpiderBase):
   
    tag_list_page_update_interval = 86400 * 365 
    first_index_page_update_interval = 10 * 60 
    index_page_update_interval = 86400 * 365 * 30 
    today_news_page_update_interval = 30 * 60 
    other_news_page_update_interval = 86400 * 365 * 30
    
    
    def __init__(self, data_adapter_config_path, encode = "utf-8"):
        SpiderBase.__init__(self, data_adapter_config_path, "yahoo_news", encode)

    def generate_content(self, main_div):
        content = []
        content_total_len = 0
            
        for para in main_div.findAll("p"):
            if len(para.text) > 20:
                content.append(para.text.strip())
                content_total_len += len(para.text)
        
        if content_total_len > 200:
            return content
        
        content = []
        text = main_div.text
        for string in main_div.strings:
            if len(string) > 20:
                content.append(string.strip())
        return content
    
    def parse_news_node(self, doc, context, created_at):
        features = {}
        if context != None and context.has_key("category"):
            features["category"] = context["category"]
        
        images = [] 
        main_div = doc.find("div", { "class" : "text fixclear" })
        if main_div != None:
            img_count = 0
            for img in main_div.findAll("img"):
                url = img.get("src")
                images.append({"name" : "img_" + str(img_count), "url" : url, "image_format" : "jpg" })
                img_count += 1
          
            features["content"] = self.generate_content(main_div) 
        
        title_div = doc.find("div", { "class" : "title" })
        if title_div != None:
            title = title_div.find("h1").text
            features["title"] = title.strip()
            span = title_div.find("span").text
            if span != None:
                for string in span.split("\n"):
                    if len(string) > 10:
                        features["reference"] = string.strip() 
                        break
        
        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.other_news_page_update_interval) 
        #if ((datetime.datetime.now() - created_at).total_seconds() <= 12 * 60 * 60):
        #    next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.today_news_page_update_interval) 
            
        return features, images, next_update_time, None 



    def parse_index_node(self, doc, context):
        category = context["category"]
        page_num = context["page_num"]
           
        children = []
        dashed_ul = doc.find("ul", { "class" : "dashed" })
        for textbox in dashed_ul.findAll("div", { "class" : "textbox" }):
            try:
                url = textbox.p.a.get("href")
                context_new = { "category" : category } 
                if not self.url_exists_in_doc_raw(url):
                    children.append({ "url" : url, 
                                      "stage" : "news", 
                                      "context" : context_new, 
                                      "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD }) 
            except BaseException, e:
                self.logger.log("error occur while adding child: %s"%(e))
        
        if len(children) > 0:
            url = "http://ent.cn.yahoo.com/event/" + category + "/index.html?page=" + str(page_num + 1) 
            context_new = { "category" : context["category"], "page_num" : page_num + 1 }
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

