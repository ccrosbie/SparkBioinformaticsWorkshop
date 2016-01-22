#big data small learning example

#load the Spark context into RStudio
.libPaths(c(.libPaths(), '/usr/lib/spark/R/lib'))  
Sys.setenv(SPARK_HOME = '/usr/lib/spark') 
library(SparkR) 
sc <- sparkR.init("local") 
sqlContext <- sparkRSQL.init(sc)

#broad institution, SNP data type, Level 3 (normalized copy number and purity/ploidy/data per sample.), 
s3url <- "s3://crosbie-tutorials/PRAD2/cgcc/broad.mit.edu/genome_wide_snp_6/snp/broad.mit.edu_PRAD.Genome_Wide_SNP_6.Level_3.108.1002.0/*"

print(s3url)

broad_SNP_6 <- SparkR:::textFile(sc, s3url)
count(broad_SNP_6) #26,087

lines <- textFile(sc, s3url)

#convert to SparkR dataframe. 
broad_SNP_6_DF <- (SparkR:::toDF(broad_SNP_6))

#what is this now?
broad_SNP_6_DF

#lets take a look
head(broad_SNP_6_DF)

#register this DataFrame as a table so we can explore with SQL 
registerTempTable(broad_SNP_6_DF,"broad_SNP_6_DF")

#only want to look at the GIRTH folders
GIRTH_DATA <- sql(sqlContext, "select _1 as line from broad_SNP_6_DF WHERE _1 like 'GIRTH%'")
head(GIRTH_DATA)

INFO <- sql(sqlContext,"select * from broad_SNP_6_DF where _1 not like 'GIRTH%'")
collect(INFO)

#now we know how to parse the fields to get the means. 
parseFields <-function(f) 
{ 
  x <- strsplit(f,"\t")
}
#lapply is like map. This is also how to scale our R functions across nodes.
xxx <- SparkR:::lapply(broad_SNP_6,parseFields)

xyz <- SparkR:::mapValues(broad_SNP_6,parseFields)

#probably better ways to do this part. Pulls into R df and does a loop. 
vals <- SparkR:::collect(xxx)
mean_vals <- numeric()
for (w in vals)
{
 for (ww in w[1])
 {
   if (ww[6] != "seg.mean")
   {
    print (ww[6]) 
    mean_vals <- append(mean_vals,as.numeric(ww[6]))
   }
  }
}
#find the mean for the entire chromosome 
print(mean(mean_vals))

#could have done this for all of TCGA. 



