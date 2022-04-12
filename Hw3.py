from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = SparkContext()
    product = sc.textFile('/tmp/bdm/keyfood_products.csv')
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
    pd_price = product.mapPartitionsWithIndex(extractproduct)
    outputTask1 = pd_price
    outputTask1.saveAsTextFile('task1_output')
