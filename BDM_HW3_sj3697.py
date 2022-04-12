import csv
import sys
import json
import numpy as np


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext





def extractproduct(partId, part):
  if partId==0:
    next(part)
  import csv
  import re
  for record in csv.reader(part):
    price = re.findall(r"[-+]?(?:\d*\.\d+|\d+)",record[5])
    temp = record[2].split('-')
    if len(price) > 0 and len(temp) > 1:
      yield (record[0], temp[-1], float(price[0]))

def extractupc(partId, part):
  if partId==0:
      next(part)
  import csv
  for record in csv.reader(part):
    temp = record[0].split('-')
    yield (temp[1], record[1])

def main(sc):
  spark = SparkSession(sc)
  product = sc.textFile('/tmp/bdm/keyfood_products.csv')
  pd_price = product.mapPartitionsWithIndex(extractproduct)
  simple_product = sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'keyfood_sample_items.csv')
  sp_upc = simple_product.mapPartitionsWithIndex(extractupc)
  df1 = spark.createDataFrame(pd_price, schema=['store','upc', 'price'])
  df2 = spark.createDataFrame(sp_upc, schema=['upc', 'name'])
  rdd_join = df1.join(df2, on='upc')
  temp=[]
  f = open('keyfood_nyc_stores.json') 
  data_json = json.load(f)
  for i in data_json.keys():
    temp.append((i,data_json[i]['communityDistrict'],round(100*data_json[i]['foodInsecurity'])))
  f.close()
  foodstore = spark.createDataFrame(temp, schema=['store','CD','FI'])
  rdd_join = rdd_join.join(foodstore, on = 'store')
  outputTask1 = rdd_join.select('name','price','FI')
  outputTask1.saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'task1_output')



if __name__ == '__main__':
  sc = SparkContext()
  main(sc)
  