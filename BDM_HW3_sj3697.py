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
  spark = SparkSession(sc)
  keyfood_store = json.load(open('keyfood_nyc_stores.json','r'))
  sample_items = dict(map(lambda x: (x[0].split('-')[-1],x[1]),
                                     pd.read_csv('keyfood_sample_items.csv').to_numpy()))

  udfGetName = F.udf(lambda x: sample_items.get(x.split('-')[-1],None), T.StringType())
  udfGetPrice = F.udf(lambda x: float(x.split()[0].lstrip('$')), T.FloatType())
  udfGetScore = F.udf(lambda x: keyfood_store[x]['foodInsecurity']*100, T.FloatType())

  outputTask1 = spark.read.csv('/tmp/bdm/keyfood_products.csv',
                               header=True, escape='"') \
                      .select(udfGetName('upc').alias('name'), udfGetPrice('price'), udfGetScore('store')) \
                      .dropna(subset=['name'])

  
  outputTask1.rdd.saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'task1_output')



if __name__ == '__main__':
  sc = SparkContext()
  main(sc)
  
