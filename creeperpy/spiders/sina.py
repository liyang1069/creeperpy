# -*- coding: utf-8 -*-
import sys

sys.path.append("../..")
import time
import scrapy
from hashlib import md5
from creeperpy.items import CreeperpyItem
from scrapy.http import HtmlResponse
from scrapy.http import Request
from hbase_handler import HbaseHandler


class SinaCreeper(scrapy.Spider):
    """docstring for SinaCreeper"""
    # def __init__(self, arg):
    #   super(SinaCreeper, self).__init__()
    #   self.arg = arg
    name = "sina_portal"
    allowed_domains = ["sina.com.cn"]
    start_urls = ["http://www.sina.com.cn/"]

    def remove_div(self, str):
        begin_index = str.find("<div")
        end_index = str.find("</div>")
        return str[0:begin_index] + str[end_index + 6:]

    def parse_item(self, response):
        item = response.meta['item']
        news_list = response.xpath('//div[@id="artibody"]').extract()
        if len(news_list) == 0:
            return
        content = news_list[0]
        item['content'] = content.replace("\r\n", "").replace("\n", "")
        while item['content'].find("<div") > -1:
            item['content'] = self.remove_div(item['content'])
        row_key = md5(item['url'].encode('utf-8')).hexdigest()
        with open("out_file/%s" % row_key, "w") as f:
            f.write(("\t".join([item['time_str'], item['url'], item['title'], item['content']]) + "\n").encode('utf-8'))
        # file = open("out_file/sina.txt", "ab")
        # try:
        #     file.write(
        #         ("\t".join([item['time_str'], item['url'], item['title'], item['content']]) + "\n").encode('utf-8'))
        # finally:
        #     file.close()
        # hbase = HbaseHandler()
        # try:
        #     md = hashlib.md5()
        #     md.update(item['url'].encode('utf-8'))
        #     if b'cf1:url' not in hbase.row("origin_news", md.hexdigest()):
        #         hbase.put("origin_news", md.hexdigest(),
        #                   {"cf1:url": item['url'], "cf1:title": item['title'], "cf1:content": item['content']})
        # finally:
        #     hbase.close()

    def parse(self, response):
        for line_a in response.xpath('//div[@class="top_newslist"]/ul/li/a'):
            item = CreeperpyItem()
            item['time_str'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            item['url'] = line_a.xpath('@href').extract()[0]
            item['title'] = line_a.xpath('text()').extract()[0]
            # print("url: %s" % item['url'])
            yield Request(item['url'], meta={'item': item}, callback=self.parse_item)
