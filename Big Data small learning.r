#load the Spark context into RStudio
.libPaths(c(.libPaths(), '/usr/lib/spark/R/lib'))  
Sys.setenv(SPARK_HOME = '/usr/lib/spark') 
library(SparkR) 
sc <- sparkR.init("local") 
sqlContext <- sparkRSQL.init(sc) 

padd_url_star = "S3://crosbie-tutorials/PRAD2/cgcc/broad.mit.edu/genome_wide_snp_6/snp/*"
#broad.mit.edu_PRAD.Genome_Wide_SNP_6.Level_3.108.1002.0/*

broad_SNP_6 <- SparkR:::textFile(sc, padd_url_star)
print(SparkR:::take(broad_SNP_6,15))
broad_SNP_6_DF <- (SparkR:::toDF(broad_SNP_6))

#could pull results into local
#localDf <- collect(broad_SNP_6_DF)
#lets you do normal R operations you cant do in SparkR
#for (i in localDf)
#{
#  print(i)
#}

#head 
head(broad_SNP_6_DF)

#schema
printSchema(broad_SNP_6_DF)

#get basic information 
broad_SNP_6_DF

#register this DataFrame as a table.
registerTempTable(broad_SNP_6_DF,"broad_SNP_6_DF")

x <- sql(sqlContext, "select ")

#only want to look at the GIRTH folders. 
GIRTH <- sql(sqlContext,"select * from broad_SNP_6_DF where _1 like 'GIRTH%' AND not like '%.gz'")
count(GIRTH)









xData <- SparkR:::textFile(sc, padd_url_star)
print(SparkR:::collect(xData)


df2 <- read.df(sqlContext, padd_url, source = "com.databricks.spark.csv")
head(df2)

df3 <- read.df(sqlContext, padd_url)
head(df3)


dff <- createDataFrame(sqlContext,padd_url)


df <- createDataFrame(sqlContext, faithful) 
head(df)

rdd <- SparkR:::textFile(sc, padd_url)
print(rdd.collect())


dfTr <- jsonFile(sqlContext, padd_url_star)
head(dfTr)

print (typeof(rdd))


SparkR:::take(5)



#df2 <- read.df(sqlContext, "README.md", source = "com.databricks.spark.csv")
#showDF(limit(df2,5))
#lines <- sqlContext.textFile(sc, padd_url)
#print(padd_url)

#big data small learning. 

