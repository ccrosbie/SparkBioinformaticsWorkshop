#from the batch ETL zeppelin notebook

%pyspark

#pull in our new unstructured dataset 
geneMap2Lines = sc.textFile("s3://crosbie-training-materials/OMIM/geneMap2.txt")

##Will register a table of the original file for QA later
row = Row("UnformattedRow")
geneMap2LinesDF = geneMap2Lines.map(row).toDF()
geneMap2LinesDF.registerTempTable("original_file")

def myColSplitter(s):
    cols = s.split("|")
    return cols
    
genesMapCols = geneMap2Lines.map(myColSplitter)

print genesMapCols.take(2)

%pyspark
from pyspark.sql.types import *

# The schema is encoded in a string.
schemaString = "CHROMOSOME.MAP_ENTRY_NUMBER MONTH_ENTERED DAY_ENTERED YEAR_ENTERED CYTOGENETIC_LOCATION GENE_SYMBOL GENE_STATUS TITLE  MIM_NUMBER METHOD COMMENTS  DISORDERS  MOUSE_CORRELATE REFERENCE"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD and cache our working dataset.
genesMapFormatted = sqlContext.createDataFrame(genesMapCols, schema)
genesMapFormatted.cache()

genesMapFormatted.show()

#Some analyst may want this raw file later. Save it off in S3 in parquet format for them. (could have also used HDFS but S3 could be be picked up by anything. (RDS, CLI, web program)
genesMapFormatted.write.parquet("s3://crosbie-training-materials/OMIM/analyst_tables/geneMap2.parquet", "overwrite")

usefulinfo = genesMapFormatted.select("GENE_SYMBOL","DISORDERS").dropDuplicates().fillna('').map(lambda useful:Row(GENE=useful.GENE_SYMBOL.split(","), DISORDER=useful.DISORDERS.split(",")))
print usefulinfo.take(5)


%pyspark
from pyspark.sql import Row

def x(y):
    ret_val = []
    for g in y.GENE:
        if g <> '':
            for d in y.DISORDER:
                if d <> '':
                    ret_val.append(Row(GENE=g,DISORDER=d))
    return ret_val
    
database_format  =  usefulinfo.map(x).filter(None)
print database_format.take(20)


database_format.toDF().write.json("s3://crosbie-training-materials/OMIM/analyst_tables/Result2.json", "overwrite")
#can move this JSON into Redshift via COPY 
