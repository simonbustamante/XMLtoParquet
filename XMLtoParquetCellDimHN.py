import sys
from Lib.XMLtoParquet import XMLtoParquet
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Argumentos de entrada del job
args = getResolvedOptions(sys.argv, ['bucket','prefix','patternZip','patternXML','backupxml'])
XMLtoParquet = XMLtoParquet(spark, args['bucket'], args['prefix'])
zip_list = XMLtoParquet.showBucketFiles(args['patternZip'])
XMLtoParquet.getXmlFromZip(zip_list, args['patternXML'])
xml_list = XMLtoParquet.showBucketFiles(args['patternXML'])
CellDimSchema = XMLtoParquet.getCellDimSchema()
XMLtoParquet.convertXMLtoParquet(xml_list,CellDimSchema)
XMLtoParquet.moveXMLFoldersToBackup(args['backupxml'],args['bucket'])

job.commit()