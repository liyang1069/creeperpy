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


class QdailyCreeper(scrapy.Spider):
    """docstring for QdailyCreeper"""
    # def __init__(self, arg):
    #   super(QdailyCreeper, self).__init__()
    #   self.arg = arg
    name = "qdaily"
    allowed_domains = ["qdaily.com"]
    start_urls = ["http://www.qdaily.com/"]

    def parse_item(self, response):
        item = response.meta['item']
        news_list = response.xpath('//div[@class="detail"]').extract()
        if len(news_list) == 0:
            return
        content = news_list[0]
        item['content'] = content.replace("\r\n", "").replace("\n", "")
        file = open("out_file/qdaily.txt", "ab")
        try:
            file.write(
                ("\t".join([item['time_str'], item['url'], item['title'], item['content']]) + "\n").encode('utf-8'))
        finally:
            file.close()
        row_key = md5(item['url'].encode('utf-8')).hexdigest()
        with open("out_file/%s" % row_key, "w") as f:
            f.write(("\t".join([item['time_str'], item['url'], item['title'], item['content']]) + "\n").encode('utf-8'))
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
        # print(response.body)
        for line_a in response.xpath('//div[@class="packery-item article"]'):
            item = CreeperpyItem()
            item['time_str'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            item['url'] = line_a.xpath('./a/@href').extract()[0]
            if item['url'].startswith('/'):
                item['url'] = "http://www.qdaily.com" + item['url']
            item['title'] = line_a.xpath('./a//img/@alt').extract()[0]
            yield Request(item['url'], meta={'item': item}, callback=self.parse_item)
