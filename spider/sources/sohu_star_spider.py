# coding=utf-8
'''
Created on Jun 3, 2012

@author: jasonz
'''
import time
import datetime 
import urllib
from spider_base import SpiderBase
from spider_base import SpiderChildNodeOperationFlag 

import re
from bs4 import BeautifulSoup

class SohuStarSpider(SpiderBase):
    '''
    classdocs
    '''
    stop_by_empty_count = 3
    star_list_page_update_interval = 86400 * 365 
    star_page_update_interval = 86400 * 365 * 60
    star_details_page_update_interval = 86400 * 365 * 10
    star_search_page_update_interval = 86400 * 30
    star_relation_page_update_interval = 86400 * 180 
    
    stage_seed = "seed"
    stage_star_list = "star_list"
    stage_star_page = "star_page"
    stage_star_details = "star_details"
    stage_star_score_page = "star_score_page"
    stage_star_films_search = "star_films_search"
    stage_star_music_search = "star_music_search"
    stage_star_relationship = "star_relationship"
    
    sohu_ent_root_url = "http://data.yule.sohu.com"
    sogo_music_root_url = "http://mp3.sogou.com/"


    def __init__(self, data_adapter_config_path, encode = "utf-8"):
        SpiderBase.__init__(self, data_adapter_config_path, "sohu_star", encode)
        #self.create_seeds()
    
#    def create_seeds(self):

    def get_key_and_value(self, text, splitter):
        self.clean_string(text)
        key, value = text.split(splitter)
        return self.clean_string(key), self.clean_string(value)
    
    def clean_string(self, text):
        text = text.replace("\n", "").strip()
        return text

    def parse_star_page(self, doc, context):
        features = {}
        children = []
        sohu_id = context['sohu_id']
        features['sohu_id'] = sohu_id
        features['title'] = context['title']
        features['score'] = context['score']
        features['attention'] = context['attention']
        features['type'] = self.stage_star_page
        
        context_new = {}
        context_new['sohu_id'] = sohu_id
        context_new['star_title'] = context['title']
        details_url = sohu_id + 'details.shtml'
        operation_flag = SpiderChildNodeOperationFlag.NEW_ADD \
                         if self.url_exists_in_doc_raw(details_url) \
                         else SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
        children.append({"url": details_url,
                         "stage" : self.stage_star_details,
                         "context": context_new,
                         "operation_flag": operation_flag})
        
        films = doc.find('a', text=re.compile(u"查看更多电影作品>>"))
        films_url = films.get('href')
        
        operation_flag = SpiderChildNodeOperationFlag.NEW_ADD \
                         if not self.url_exists_in_doc_raw(films_url) \
                         else SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
        children.append({"url": films_url,
                         "stage" : self.stage_star_films_search,
                         "context": context_new,
                         "operation_flag": operation_flag})
        
        music = doc.find('a', text=re.compile(u"查看更多专辑>>"))
        music_url = music.get('href')
        operation_flag = SpiderChildNodeOperationFlag.NEW_ADD \
                         if not self.url_exists_in_doc_raw(music_url) \
                         else SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
        children.append({"url": music_url,
                         "stage" : self.stage_star_music_search,
                         "context": context_new,
                         "operation_flag": operation_flag})
        
        relation = doc.find('a', text=re.compile(u"明星关系圈"))
        if (relation != None):
            relation_url = self.sohu_ent_root_url + \
                           urllib.quote(unicode(relation.get('href')).encode('gb18030'), ":?=/-+%")
            operation_flag = SpiderChildNodeOperationFlag.NEW_ADD \
                         if not self.url_exists_in_doc_raw(relation_url) \
                         else SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
            print "relation_operation", operation_flag
            children.append({"url": relation_url,
                             "stage" : self.stage_star_relationship,
                             "context": context_new,
                             "operation_flag": operation_flag})
        
        next_update_time = datetime.datetime.now() + \
                           datetime.timedelta(0, self.star_page_update_interval)
        images = []
        return features, images, next_update_time, children 


    def parse_star_films_search(self, doc, context):
        if doc.find(text=re.compile(u"没有找到相关结果")) != None:
            return None, None, None, None
        features = {}
        movies = []
        tvs = []
        movie_div = doc.find('div', {'class':"block clear"})
        movie_blocks = movie_div.find_all('div', {'class': 'cont r'})
        for movie_item in movie_blocks:
            movie_name = self.clean_string(movie_item.a.get_text())
            movie_url = movie_item.a.get('href')
            if movie_item.em.get_text() == u"电视":
                tvs.append({"url":movie_url,
                            "name":movie_name})
            elif movie_item.em.get_text() == u"电影":
                movies.append({"url":movie_url,
                            "name":movie_name})
        features['sohu_id'] = context['sohu_id']
        features['star_title'] = context['star_title']
        features['movies'] = movies
        features['tvs'] = tvs
        features['type'] = self.stage_star_films_search
        children = []
        next_page_tag = doc.find("a", text=re.compile(u"下一页"))
        if next_page_tag != None:
            url = self.sohu_ent_root_url + "/movie/" + next_page_tag.get('href')
            operation_flag = SpiderChildNodeOperationFlag.NEW_ADD
            if self.url_exists_in_doc_raw(url):
                operation_flag = SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
            children.append({'url': url,
                             'stage': self.stage_star_films_search,
                             'context' : context,
                             'operation_flag' : operation_flag})
        next_update_time = datetime.datetime.now() + \
                           datetime.timedelta(0, self.star_search_page_update_interval)
        return features, None, next_update_time, children
        
    
    def parse_star_music_search(self, doc, context):
        if doc.find(text=re.compile(u"没有找到")) != None:
            return None, None, None, None
        features = {}
        albums = []
        album_blocks = doc.find_all('table', {"class":"spbox"})
        for album_item in album_blocks:
            al = album_item.find("td", {"colspan" : "8"})
            album_url = al.a.get('href')
            album_name = al.a.get_text().replace(u"《", "").replace(u"》", "")
            album_name = self.clean_string(album_name)
            albums.append({"album_name": album_name,
                           "album_url" : album_url})
        
        features['sohu_id'] = context['sohu_id']
        features['star_title'] = context['star_title']
        features['albums'] = albums
        features['type'] = self.stage_star_music_search
        children = []
        next_page_tag = doc.find("a", text=re.compile(u"下一页"))
        if next_page_tag != None:
            url = self.sogo_music_root_url + next_page_tag.get('href')
            operation_flag = SpiderChildNodeOperationFlag.NEW_ADD
            if self.url_exists_in_doc_raw(url):
                operation_flag = SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
            children.append({'url': url,
                             'stage': self.stage_star_music_search,
                             'context' : context,
                             'operation_flag' : operation_flag})
        next_update_time = datetime.datetime.now() + \
                           datetime.timedelta(0, self.star_search_page_update_interval)
        return features, None, next_update_time, children

    def parse_star_relation(self, doc, context):
        features = {}
        relations = []
        relation_blocks = doc.find("table", {'id':'list1'}).find_all("tr")
        for relation in relation_blocks:
            fields = relation.find_all('td')
            relations.append({"relation":self.clean_string(fields[2].get_text()),
                              "object":self.clean_string(fields[3].get_text())})
        features['sohu_id'] = context['sohu_id']
        features['star_title'] = context['star_title']
        features['relations'] = relations
        features['type'] = self.stage_star_relationship
        next_update_time = datetime.datetime.now() + \
                           datetime.timedelta(0, self.star_relation_page_update_interval)
        return features, None, next_update_time, None

    def parse_star_list(self, doc, context):
        children = []
        tags = doc.find_all("div", {"class":"pt"})
        for tag in tags:
            url = self.sohu_ent_root_url + tag.div.a.get('href')
            title = tag.div.a.get('title')
            score_text = tag.find(text=re.compile(u"评分"))
            score_value = self.get_key_and_value(score_text, u"：")[1]
            attention_text = tag.find(text=re.compile(u"关注度"))
            attention_value = self.get_key_and_value(attention_text, u"：")[1]
            print score_value, attention_value, type(score_value), type(attention_value)
            context_new = {}
            context_new['title'] = title
            context_new["score"] = float(score_value)
            context_new['attention'] = float(attention_value)
            context_new['sohu_id'] = url
            
            operation_flag = SpiderChildNodeOperationFlag.NEW_ADD
            if self.url_exists_in_doc_raw(url):
                operation_flag = SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
            children.append({ "url" : url, 
                              "stage" : self.stage_star_page, 
                              "context" : context_new, 
                              "operation_flag" : SpiderChildNodeOperationFlag.NEW_ADD })

        next_page_tag = doc.find("a", text=u"下一页")
        if next_page_tag != None:
            url = self.sohu_ent_root_url + next_page_tag.get('href')
            print url
            operation_flag = SpiderChildNodeOperationFlag.NEW_ADD
            if self.url_exists_in_doc_raw(url):
                operation_flag = SpiderChildNodeOperationFlag.UPDATE_INFO_ONLY
            context_new_2 = {}
            children.append({ "url" : url, 
                              "stage" : self.stage_star_list, 
                              "context" : context_new_2, 
                              "operation_flag" : operation_flag })
        else :
            self.logger.log("No next page")

        next_update_time = datetime.datetime.now() + \
                           datetime.timedelta(0, self.star_list_page_update_interval) 
        return None, None, next_update_time, children
        
    
    def parse_star_detailed_page(self, doc, context):
        features = {}
        features['sohu_id'] = context['sohu_id']
        features['type'] = self.stage_star_details
        info = doc.find('a', {'name':'grda'})
        if info !=None:
            info = info.find_parent("div")
            for item in info.contents:
                if hasattr(item, 'get_text'):
                    item_text = item.get_text()
                    key = ""
                    value = ""
                    if item_text.find(u"：") != -1:
                        key, value = self.get_key_and_value(item_text, u"：")
                    elif item_text.find(":") != -1:
                        key, value = self.get_key_and_value(item_text, u":")
                    if key != "" and value != "":
                        features[key]=[]
                        features[key].append(value)
        else:
            raise Exception("No personal info for " + str(context['sohu_id']))
        key = ""
        value = ""
        resume = doc.find('div', {'class':'txtRA'})
        if resume != None:
            for item in resume.contents:
                if hasattr(item, 'name') and hasattr(item, 'get_text'):
                    if item.name == 'h3':
                        key = self.clean_string(item.get_text().replace(u"：", ""))
                    if item.name == 'p':
                        value += self.clean_string(item.get_text())
            if key != "" and value != "":
                features[key] = []
                features[key].append(value)
        img = doc.find_all('a', {'href':'index.shtml'})[1]
        images = {}
        if img!= None:
            images.append({'name': 'icon',
                           'url': self.sohu_ent_root_url + '/' +img.img.get('src'),
                           'image_format': 'jpg'})

        next_update_time = datetime.datetime.now() + \
                           datetime.timedelta(0, self.star_details_page_update_interval) 
        return features, images, next_update_time, None

        
    def parse(self, url_hash, page, encode, stage, context, created_at, page_crawled_at):
        doc = BeautifulSoup(page)
        if stage == self.stage_star_list:
            return self.parse_star_list(doc, context)
        elif stage == self.stage_star_page:
            return self.parse_star_page(doc, context)
        elif stage == self.stage_star_films_search:
            return self.parse_star_films_search(doc, context)
        elif stage == self.stage_star_music_search:
            return self.parse_star_music_search(doc, context)
        elif stage == self.stage_star_relationship:
            return self.parse_star_relation(doc, context)
        return None, None, None, None
    
