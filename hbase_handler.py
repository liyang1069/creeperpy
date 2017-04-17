# -*- coding: utf-8 -*-
import happybase

class HbaseHandler(object):
  """docstring for HbaseHandler"""
  def __init__(self):
    self.connection = happybase.Connection()
    self.connection.open()

  def close(self):
    self.connection.close()
    
  def test(self):
    print(self.connection.tables())

  def createTable(self, tableName, families):
    self.connection.create_table(tableName, families)

  def getTable(self, tableName):
    return self.connection.table(tableName)

  def put(self, tableName, rowKey, value):
    table = self.getTable(tableName)
    table.put(rowKey, value)

  def row(self, tableName, rowKey):
    table = self.getTable(tableName)
    return table.row(rowKey)

# h = HbaseHandler()
# families = {'cf1': dict(max_versions=1),}
# h.createTable('origin_news', families)
# h.close()