'''
Created on May 16, 2012

@author: jasonz
'''
import time
from multiprocessing import Process
from multiprocessing import JoinableQueue
import sys
sys.path.append("../")
from common.image_store_adapter import ImageStoreAdapter
from common.image_store_adapter import ImageIndexStatus
from common.logger import Logger
from common import common_utils
class ImageCrawler:
    
    NUM_PER_FETCH = 100
    NUM_PROCESSES = 10
    def __init__(self, database_config_path):
        self.queue = JoinableQueue()
        self.logger = Logger("image_crawler")
        self.adapter = ImageStoreAdapter(database_config_path, self.logger)
        
    def produce(self):
        while True:
            if self.queue.empty():
                for image_id, link in self.adapter.load_undownloaded_images(self.NUM_PER_FETCH):
                    self.logger.log("Producer: add new image to crawl:" + image_id + " " + link)
                    self.queue.put((image_id, link))
            time.sleep(10)
            
    def consume(self, process_id):
        while True:
            self.logger.log("Consumer process:" + str(process_id) + " fetch new image from queue")
            if not self.queue.empty():
                image_id, link = self.queue.get()
                self.logger.log("Consumer process:"+ str(process_id) + " start crawling " + str(link))
                image = common_utils.page_crawl(link)
                if image != None:
                    self.logger.log(link + "crawled successfully")
                    self.adapter.store_image(image_id, image)
                else:
                    self.logger.log(link + " failed at crawling")
                    self.adapter.update_image_status(image_id, ImageIndexStatus.DOWNLOAD_FAILED)
                self.queue.task_done()
                time.sleep(1)
            else:
                self.logger.log("Queue empty")
                time.sleep(10)
    
    def run(self):
        producer = Process(target=self.produce)
        producer.start()
        consumers = []
        for i in range(self.NUM_PROCESSES):
            consumer = Process(target=self.consume, args=(i,))
            consumers.append(consumer)
            consumer.start()
        
        for consumer in consumers:
            consumer.join()
        producer.join()
        self.queue.join()
