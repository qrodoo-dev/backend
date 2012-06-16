'''
Created on May 15, 2012

@author: jasonz
'''

from image_crawler import ImageCrawler
if __name__ == '__main__':
    crawler = ImageCrawler("../database_config.xml")
    crawler.run()
    
