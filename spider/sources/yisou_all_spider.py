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

class YisouAllSpider(SpiderBase):
   
    
    index_page_update_interval = 86400 * 14 
    movie_page_update_interval = 86400 * 365 * 30 
    people_page_update_interval = 86400 * 20 
    
    def __init__(self, data_adapter_config_path, encode = "utf-8"):
        SpiderBase.__init__(self, data_adapter_config_path, "yisou_all", encode)


    def get_children_from_movie_list(self, resultList):
        for result in resultList.findAll("li"):
            try:
                text_info = result.find("div", { "class" : "text-info" })
                top_area = result.find("div", { "class" : "top-wrap clearfix" })
                context_new = {}
                if top_area != None:
                    for a in top_area.findAll("a"):
                        a_class = a.get("class")
                        if a_class != None and a_class.count("sub-title"):
                            context_new["abstract"] = a.text
                            break
                
                middle_div = text_info.find("div", { "class" : "middle" })
                summary_div = middle_div.find("dd", { "class" : "summary" })
                a = summary_div.find("a")
                url = a.get("href")
            
                operation_flag = SpiderChildNodeOperationFlag.NEW_ADD
                if self.url_exists_in_doc_raw(url):
                    operation_flag = SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
                
                child = { "url" : url, 
                          "stage" : "movie", 
                          "context" : context_new, 
                          "operation_flag" : operation_flag }
                yield child
            
            except BaseException, e:
                self.logger.log("error occur while getting child from movie-list: %s"%(e))
   
    def get_next_page_from_movie_list(self, resultList, context):
        child_next_page = None
        page_wrap = resultList.find("div", { "class" : "page-wrap" })
        if page_wrap != None:
            for a in page_wrap.findAll("a"):
                if a.text == u"下一页":
                    url = a.get("href")
                    if not self.url_exists_in_doc_raw(url):
                        page_num = context["page_num"] + 1
                        context_new = { "page_num" : page_num } 
                        child_next_page = { "url" : url, 
                                            "stage" : "movie_index", 
                                            "context" : context_new, 
                                            "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD }
                    break
        return child_next_page

    def parse_movie_index(self, doc, context):
        children = []
        resultList = doc.find("div", { "id" : "resultList" })
        if resultList != None:
            for child in self.get_children_from_movie_list(resultList):
                children.append(child)
        
            child_next_page = self.get_next_page_from_movie_list(resultList, context)
            if child_next_page != None:
                children.append(child_next_page)
        
        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.index_page_update_interval) 
        return None, None, next_update_time, children
    
    def parse_movie(self, doc, context):
        features = {}
        images = [] 
        children = []
       
        features["category"] = "movie"
        if context.has_key("abstract") and context["abstract"] != None:
            features["abstract"] = context["abstract"]


        intro_div = doc.find("div", { "class" : "main" }).find("div", { "class" : "intro cf" })
        
        img_a = intro_div.find("a")
        image_link = img_a.find("img").get("src")
        images.append({"name" : "cover", "url" : image_link, "image_format" : "jpg" })
        video_length = img_a.find("div", { "class" : "intro_length" })
        if video_length != None:
            features["video_length"] = video_length.text

        detail_div = intro_div.find("div", { "class" : "intro_detail" })
        title = detail_div.find("div", { "class" : "intro_top cf" }).text.strip()
        features["title"] = title

        detail_content_div = detail_div.find("div", { "class" : "intro_middle" })
        for dl in detail_content_div.findAll("dl"):
            key = None
            for ele in dl.findAll():
                if ele.name == "dt":
                    key = ele.text
                elif ele.name == "dd":
                    if key != None:
                        if ele.get("class") != None and ele.get("class").count("intro_summary") > 0:
                            detail_div = ele.find("p", { "class" : "detail" })
                            if detail_div == None:
                                detail_div = ele.find("p", { "class" : "brief" })
                            detail_text = detail_div.text.strip()
                            features[key] = detail_text.replace(u"\r\n　　", "") 
                        elif key == u"主演：" or key == u"导演：":
                            content = [] 
                            for a in ele.findAll("a"):
                                url = "http://v.yisou.com" + a.get("href")
                                content.append({"name" : a.text.strip(), "url" : url})
                                if not self.url_exists_in_doc_raw(url):
                                    children.append({ "url" : url, 
                                                      "stage" : "people", 
                                                      "context" : None, 
                                                      "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD })
                            features[key] = content
                        else:
                            features[key] = ele.text.strip() 
                            
                    key = None
        
        play_switch_ul = detail_div.find("ul", { "class" : "cf switch" })
        if play_switch_ul != None:
            features["play_links"] = [] 
            for a in play_switch_ul.findAll("a"):
                features["play_links"].append({"name" : a.text,  "url" : a.get("href")})

        related_search_div = doc.find("div", { "class" : "cf rel mt30" })
        if related_search_div != None:
            try:
                features["related_entity"] = [] 
                for a in related_search_div.findAll("a"):
                    url = "http://v.yisou.com" + a.get("href")
                    name = a.find("span").text.strip()
                    features["related_entity"].append({"name" : name, "url" : url})
            except BaseException, e:
                self.logger.log("error occur while getting related search entities: %s"%(e))

        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.movie_page_update_interval) 
        
        return features, images, next_update_time, children 
        
    def parse_people(self, doc):
        features = {}
        images = []

        features["category"] = "people"
        
        intro_pic_div = doc.find("div", { "class" : "intro_pic" })
        image_link = intro_pic_div.find("img").get("src")
        images.append({"name" : "avatar", "url" : image_link, "image_format" : "jpg" })
        
                
        intro_detail_div = doc.find("div", { "class" : "intro_detail" })
        features["title"] = intro_detail_div.find("div", { "class" : "intro_top cf" }).text.strip()
    
        for dl in intro_detail_div.find("div", { "class" : "intro_middle" }).findAll("dl"):
            key = None
            for ele in dl.findAll():
                if ele.name == "dt":
                    key = ele.text.strip()
                elif ele.name == "dd":
                    features[key] = ele.text.strip()
        
        works_list_div = doc.find("div", { "class" : "works_list cf"})
        if works_list_div != None:
            features["related_entity"] = [] 
            for dd in works_list_div.findAll("dd"):
                a = dd.find("a")
                if a != None:
                    url = "http://v.yisou.com" + a.get("href")
                    features["related_entity"].append({"name" : a.text.strip(), "url" : url})
        
        next_update_time = datetime.datetime.now() + datetime.timedelta(0, self.people_page_update_interval) 
        
        return features, images, next_update_time, None 

    def parse(self, url_hash, page, encode, stage, context, created_at, page_crawled_at):
        doc = BeautifulSoup(page)
        if stage == "movie_index":
            return self.parse_movie_index(doc, context)
        elif stage == "movie":
            return self.parse_movie(doc, context)
        elif stage == "people":
            return self.parse_people(doc)
            
        return None, None, None, None

