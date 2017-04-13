# -*- coding: utf-8 -*-
#hadoop fs -put qq.txt /newscreeper/qq-20170413.txt
import time
from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("uniquness news").config("master", "local[*]").getOrCreate()
sc = spark.sparkContext
date_str = time.strftime("%Y%m%d", time.localtime())
file_path = "hdfs://localhost:9000/newscreeper/qq-" + date_str + ".txt"
lines = sc.textFile(file_path)
parts = lines.map(lambda l: l.split("\t"))

news = parts.map(lambda p: Row(time_str = p[0], url = p[1], title = p[2], content = p[3]))

schemaNews = spark.createDataFrame(news)
schemaNews.createOrReplaceTempView("news")

uniq_urls = spark.sql("SELECT distinct url from news").rdd.map(lambda p: p.url).collect()
print("===================uniq_urls: %d" % len(uniq_urls))
uniq_news_list = news.filter(lambda n: uniq_urls.index(n.url) > -1)

def filter_handler(n):
  if n.url in uniq_urls:
    uniq_urls.remove(n.url)
    return True
  else:
    return False

uniq_news_list = news.filter(filter_handler).map(lambda p: "\t".join([p.time_str, p.url, p.title, p.content])).collect()
print("===================uniq_news_list: %d" % len(uniq_news_list))
file = open("out_file/unqi.txt","wb")
try:
  for one_news in uniq_news_list:
    file.write((one_news + "\n").encode('utf-8'))
finally:
  file.close()