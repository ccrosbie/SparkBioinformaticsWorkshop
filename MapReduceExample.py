%pyspark
#This example simply demonstrates the speed Spark can run map reduce jobs

#To run PySpark in EMR add spark.executorEnv.PYTHONPATH=/usr/lib/spark/python/lib/py4j-0.8.2.1-src.zip:/usr/lib/spark/python/:{{PWD}}/pyspark.zip{{PWD}}/py4j-0.8.2.1-src.zip

## Zeppelin creates and injects sc (SparkContext) and sqlContext (HiveContext or SqlContext)
## So you don't need create them manually

genes = sc.textFile("s3://crosbie-training-materials/OMIM/geneMap2.txt")
print type(genes)

#sc = spark context = Main entry point for Spark functionality. 
#A SparkContext represents the connection to a Spark cluster, 
#and can be used to create RDDs, accumulators and broadcast variables on that cluster.

print genes.count()
print "-------------"
print genes.first()

def find_max(a, b):
     if a > b:
         return a
     else:
         return b

wordCounts = genes.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(find_max)

#Map = Return a new RDD by first applying a function to all elements of this RDD
#Flat Map = Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.
#reduceByKey = Merge the values for each key using an associative reduce function.
#This will also perform the merging locally on each mapper before sending results to a 
#reducer, similarly to a “combiner” in MapReduce.

wordCounts.cache()
print wordCounts.count()
print wordCounts.collect()

wordCountDF = wordCounts.toDF()
print type(wordCountDF)






