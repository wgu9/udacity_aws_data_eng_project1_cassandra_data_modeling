import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node trusted
trusted_node1741212780382 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="trusted_node1741212780382")

# Script generated for node accelerometer_landing
accelerometer_landing_node1741213136243 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1741213136243")

# Script generated for node SQL Query
SqlQuery2497 = '''
SELECT DISTINCT ct.*
FROM customer_trusted ct
JOIN accelerometer_landing at ON ct.email = at.user
WHERE ct.sharewithresearchasofdate IS NOT NULL;
'''
SQLQuery_node1741213309929 = sparkSqlQuery(glueContext, query = SqlQuery2497, mapping = {"accelerometer_landing":accelerometer_landing_node1741213136243, "customer_trusted":trusted_node1741212780382}, transformation_ctx = "SQLQuery_node1741213309929")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1741213309929, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741212082148", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1741212876636 = glueContext.getSink(path="s3://jeremy-udacity-spark/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1741212876636")
AmazonS3_node1741212876636.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
AmazonS3_node1741212876636.setFormat("json")
AmazonS3_node1741212876636.writeFrame(SQLQuery_node1741213309929)
job.commit()