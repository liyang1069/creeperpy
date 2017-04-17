# -*- coding: utf-8 -*-
# def main():
#   # Make socket
#   transport = TSocket.TSocket('localhost', 9090)

#   # Buffering is critical. Raw sockets are very slow
#   transport = TTransport.TBufferedTransport(transport)

#   # Wrap in a protocol
#   protocol = TBinaryProtocol.TBinaryProtocol(transport)

#   # Create a client to use the protocol encoder
#   client = Calculator.Client(protocol)

#   # Connect!
#   transport.open()

#   client.ping()
#   print('ping()')

#   sum_ = client.add(1, 1)
import sys;
sys.path.append("..")
from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase.hbase import Hbase
from hbase.hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation, TRegionInfo
from hbase.hbase.ttypes import IOError, AlreadyExists

#server端地址和端口
transport = TSocket.TSocket("localhost", 9095)
#可以设置超时
transport.setTimeout(5000)
#设置传输方式（TFramedTransport或TBufferedTransport）
trans = TTransport.TBufferedTransport(transport)
#设置传输协议
protocol = TBinaryProtocol.TBinaryProtocol(trans)
#确定客户端
client = Hbase.Client(protocol)
#打开连接
transport.open()

#获取表名
client.getTableNames()
#创建新表
_TABLE = "keyword"
demo = ColumnDescriptor(name='data:',maxVersions = 10)#列族data能保留最近的10个数据，每个列名后面要跟:号
createTable(_TABLE, [demo])

#创建列名2个data:url data:word  
tmp1= [Mutation(column="data:url", value="www.baidu.com")]
tmp2= [Mutation(column="data:word", value="YaGer")]
#新建2个列 (表名，行键， 列名)
client.mutateRow(_TABLE, row, tmp1)
client.mutateRow(_TABLE, row, tmp1)
#从表中取数据
#通过最大版本数取数据
client.getByMaxver(_TABLE,'00001','data:word', 10)#一次取10个版本
#取列族内数据
client.getColumns(_TABLE, '00001')