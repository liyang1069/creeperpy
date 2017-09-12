# -*- coding: utf-8 -*-
import sys

sys.path.append("../..")
import time
import scrapy
from hashlib import md5
import json
from creeperpy.items import CreeperpyItem
from scrapy.http import HtmlResponse
from scrapy.http import Request
from hbase_handler import HbaseHandler
# from kafka import KafkaProducer
# from kafka.errors import KafkaError

# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# topic = "creeper"


class QQCreeper(scrapy.Spider):
    """docstring for QQCreeper"""
    # def __init__(self, arg):
    #   super(QQCreeper, self).__init__()
    #   self.arg = arg
    name = "qqnews"
    allowed_domains = ["qq.com"]
    start_urls = ["http://news.qq.com/"]

    # Asynchronous by default
    # future = producer.send('creeper', b'raw_bytes')

    def parse_item(self, response):
        item = response.meta['item']
        news_list = response.xpath('//div[@id="Cnt-Main-Article-QQ"]').extract()
        if len(news_list) == 0:
            return
        content = news_list[0]
        item['content'] = content.replace("\r\n", "").replace("\n", "")
        # global producer, topic
        # file = open("out_file/qq.txt", "ab")
        # try:
        #     file.write(
        #         ("\t".join([item['time_str'], item['url'], item['title'], item['content']]) + "\n").encode('utf-8'))
        # finally:
        #     file.close()

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
        # producer.send(topic, json.dumps(
        #     {"cf1:url": item['url'], "cf1:title": item['title'], "cf1:content": item['content']}).encode())

    def parse(self, response):
        for line_a in response.xpath('//div[@class="item major"]//a[@class="linkto"]'):
            item = CreeperpyItem()
            item['time_str'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            item['url'] = line_a.xpath('@href').extract()[0]
            item['title'] = line_a.xpath('text()').extract()[0]
            yield Request(item['url'], meta={'item': item}, callback=self.parse_item)

        # global producer, topic
        # producer.flush()
