# coding=utf-8
'''
Created on Apr 25, 2012

@author: jasonz
'''
import gzip 
import StringIO
import md5
import urllib2
import time
        
def compress(content):
    '''compress content using gzip'''
    zbuf = StringIO.StringIO()
    zfile = gzip.GzipFile(mode='wb', compresslevel=9, fileobj=zbuf)
    zfile.write(content)
    zfile.close()
    return zbuf.getvalue()

def uncompress(data):
    '''uncompress gzip data'''
    zbuf = StringIO.StringIO(data)
    zfile = gzip.GzipFile(mode = 'r', compresslevel = 9, fileobj = zbuf)
    content = zfile.read()
    return content

def gen_url_hash(url):
    return md5.new(url).hexdigest()
    
def page_crawl(url, retry=3):
    retry_times = 0
    page = None
    while retry_times <= retry:
        retry_times += 1
        try:
            page = urllib2.urlopen(url, timeout=20).read()
        except:
            print "error occurred when fetching ", url
            time.sleep(2)
            continue
        break
    return page

