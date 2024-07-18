import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: ['JOB_NAME','S3_INPUT_PATH','S3_OUTPUT_PATH','KEY_COLUMN_NAME','VALUE_COLUMN_NAME']
args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_INPUT_PATH','S3_OUTPUT_PATH','KEY_COLUMN_NAME','VALUE_COLUMN_NAME'])

#Initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Define variables from parameters
imput_path = args['S3_INPUT_PATH']
output_path = args['S3_OUTPUT_PATH']
key_column_name = args['KEY_COLUMN_NAME']
value_column_name = args['VALUE_COLUMN_NAME']

#Read articles.csv from S3
df_articles = spark.read.option("header","true").csv(imput_path)

# Generate dictionary dataframe
df_dictionary = df_articles.select(key_column_name, value_column_name).distinct()

#Write diccionary as csv in S3
df_dictionary.write.option("header", "true").mode("overwrite").csv(output_path)

# Read the dictionary csv from S3
df_readed = spark.read.option("header", "true").csv(output_path)

# Perform inner join with the original articles dataframe
df_joined = df_articles.join(df_readed, on=[key_column_name, value_column_name], how='inner')

#Print for debug number of rows
print("The number of rows in the original dataframe is ", df_articles.count())
print("The number of rows in the joined dataframe is ", df_joined.count())

#Commit
job.commit()