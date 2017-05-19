# -*- coding: utf-8 -*-
import sys
sys.path.append("../..")
import time
import scrapy
import hashlib
from creeperpy.items import CreeperpyItem
from scrapy.http import HtmlResponse
from scrapy.http import Request
from hbase_handler import HbaseHandler

class QQCreeper(scrapy.Spider):
  """docstring for QQCreeper"""
  # def __init__(self, arg):
  #   super(QQCreeper, self).__init__()
  #   self.arg = arg
  name = "qq"
  allowed_domains = ["qq.com"]
  start_urls = ["http://www.qq.com"]

  def parse_item(self, response):
    item = response.meta['item']
    news_list = response.xpath('//div[@id="Cnt-Main-Article-QQ"]').extract()
    if len(news_list) == 0:
      return
    content = news_list[0]
    item['content'] = content.replace("\r\n","").replace("\n","")
    # file = open("out_file/qq.txt","ab")
    # try:
    #   file.write(("\t".join([item['time_str'], item['url'], item['title'], item['content']]) + "\n").encode('utf-8'))
    # finally:
    #   file.close()
    hbase = HbaseHandler()
    try:
      md = hashlib.md5()
      md.update(item['url'].encode('utf-8'))
      if b'cf1:url' not in hbase.row("origin_news", md.hexdigest()):
        hbase.put("origin_news", md.hexdigest(), {"cf1:url": item['url'], "cf1:title": item['title'], "cf1:content": item['content']})
    finally:
      hbase.close()

  def parse(self, response):
    for line_a in response.xpath('//div[@id="newsInfoQuanguo"]//ul/li/a'):
      item = CreeperpyItem()
      item['time_str'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
      item['url'] = line_a.xpath('@href').extract()[0]
      item['title'] = line_a.xpath('text()').extract()[0]
      yield Request(item['url'], meta={'item': item}, callback=self.parse_item)
    