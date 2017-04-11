#coding=utf-8
import time
import scrapy
from creeperpy.items import CreeperpyItem
from scrapy.http import HtmlResponse
from scrapy.http import Request

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
    content = response.xpath('//div[@id="Cnt-Main-Article-QQ"]').extract()[0]
    item['content'] = content.replace("\r\n","").replace("\n","")
    file = open("out_file/qq.txt","a")
    try:
      file.write("\t".join([item['time'], item['url'], item['title'], item['content']]) + "\n")
    finally:
      file.close()

  def parse(self, response):
    for line_a in response.xpath('//div[@id="newsInfoQuanguo"]//ul/li/a'):
      item = CreeperpyItem()
      item['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
      item['url'] = line_a.xpath('@href').extract()[0]
      item['title'] = line_a.xpath('text()').extract()[0]
      yield Request(item['url'], meta={'item': item}, callback=self.parse_item)
    