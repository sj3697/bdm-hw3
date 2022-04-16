import csv
import sys
import json
import numpy as np
import pandas as pd



import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext



def main(sc):
  #spark = SparkSession(sc)
  keyfood_store = json.load(open('keyfood_nyc_stores.json','r'))
  sample_items = dict(map(lambda x: (x[0].split('-')[-1],x[1]),
                                     pd.read_csv('keyfood_sample_items.csv').to_numpy()))

  def readProducts(partId, part):
    if partId == 0: next(part)
    for x in csv.reader(part):
      itemsName = sample_items.get(x[2].split('-')[-1], None)
      if itemsName:
        yield(itemsName,
              float(x[5].split()[0].lstrip('$')),
              keyfood_store[x[0]]['foodInsecurity']*100)
  outputTask1 = sc.textFile('/tmp/bdm/keyfood_products.csv') \
                .mapPartitionsWithIndex(readProducts)

  
  outputTask1.saveAsTextFile('Task1_output')



if __name__ == '__main__':
  sc = SparkContext()
  main(sc)
  
