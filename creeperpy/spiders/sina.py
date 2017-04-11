#coding=utf-8
import scrapy
from creeperpy.items import CreeperpyItem

class SinaCreeper(scrapy.Spider):
  """docstring for SinaCreeper"""
  # def __init__(self, arg):
  #   super(SinaCreeper, self).__init__()
  #   self.arg = arg
  name = "sina_portal"
  allowed_domains = ["sina.com.cn"]
  start_urls = ["http://www.sina.com.cn/"]

  def parse(self, response):
    news_top = response.xpath('//news_top')
    print(response)
    for line_a in news_top.xpath('/li/a'):
      item = CreeperpyItem()
      item['time'] = 1
      item['url'] = line_a.xpath('@href').extract()
      item['title'] = line_a.xpath('text()').extract()
      item['content'] = line_a.xpath('text()').extract()
      yield item
