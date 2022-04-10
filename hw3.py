import csv
import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import IPython
%matplotlib inline
IPython.display.set_matplotlib_formats('svg')
pd.plotting.register_matplotlib_converters()
sns.set_style("whitegrid")

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T



def extractprice(partId, part):
  if partId==0:
    next(part)
  import csv
  import re
  for record in csv.reader(part):
    price = re.findall(r"[-+]?(?:\d*\.\d+|\d+)",record[5])
    temp = record[2].split('-')
    if len(price) > 0 and len(temp) > 1:
      yield (record[0], temp[1], float(price[0]))
    elif len(price) > 0 and len(temp) == 1:
      yield (record[0],temp[0], float(price[0]))

def extractupc(partId, part):
  if partId==0:
      next(part)
  import csv
  for record in csv.reader(part):
    temp = record[0].split('-')
    yield (temp[1], record[1])

sp_upc = simple_product.mapPartitionsWithIndex(extractupc)
sp_upc.take(2)


if __name__ == '__main__':
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession(sc)
    product = sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'keyfood_products')
    pd_price = product.mapPartitionsWithIndex(extractprice)
    simple_product = sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'keyfood_sample_items')
    df1 = spark.createDataFrame(pd_price, schema=['store','upc', 'price'])
    df2 = spark.createDataFrame(sp_upc, schema=['upc', 'name'])
    rdd_join = df1.join(df2, on='upc')
    saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'output')
