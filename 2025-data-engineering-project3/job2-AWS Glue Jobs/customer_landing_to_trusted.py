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

# Script generated for node Amazon S3
AmazonS3_node1741155335247 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jeremy-udacity-spark/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1741155335247")

# Script generated for node SQL Query - filter step
SqlQuery3247 = '''
select * from myDataSource
where 
sharewithresearchasofdate is not null
'''
SQLQueryfilterstep_node1741157310079 = sparkSqlQuery(glueContext, query = SqlQuery3247, mapping = {"myDataSource":AmazonS3_node1741155335247}, transformation_ctx = "SQLQueryfilterstep_node1741157310079")

# Script generated for node Trusted Customer Zone
EvaluateDataQuality().process_rows(frame=SQLQueryfilterstep_node1741157310079, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741153287725", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomerZone_node1741155868970 = glueContext.getSink(path="s3://jeremy-udacity-spark/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1741155868970")
TrustedCustomerZone_node1741155868970.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedCustomerZone_node1741155868970.setFormat("json")
TrustedCustomerZone_node1741155868970.writeFrame(SQLQueryfilterstep_node1741157310079)
job.commit()