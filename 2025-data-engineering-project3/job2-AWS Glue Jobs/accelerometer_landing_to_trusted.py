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

# Script generated for node accelerometer landing
accelerometerlanding_node1741207992690 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jeremy-udacity-spark/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1741207992690")

# Script generated for node Customer Trusted
CustomerTrusted_node1741207991607 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1741207991607")

# Script generated for node Step 2. Joins and Filters
SqlQuery2845 = '''
SELECT DISTINCT a.* 
FROM stedi.accelerometer_landing a
INNER JOIN customertrusted c
ON a.user = c.email
'''
Step2JoinsandFilters_node1741208058715 = sparkSqlQuery(glueContext, query = SqlQuery2845, mapping = {"customertrusted":CustomerTrusted_node1741207991607, "accelerometerlanding":accelerometerlanding_node1741207992690}, transformation_ctx = "Step2JoinsandFilters_node1741208058715")

# Script generated for node Step 3. Create accelerometer_trusted
EvaluateDataQuality().process_rows(frame=Step2JoinsandFilters_node1741208058715, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741207416468", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Step3Createaccelerometer_trusted_node1741208368283 = glueContext.getSink(path="s3://jeremy-udacity-spark/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step3Createaccelerometer_trusted_node1741208368283")
Step3Createaccelerometer_trusted_node1741208368283.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
Step3Createaccelerometer_trusted_node1741208368283.setFormat("json")
Step3Createaccelerometer_trusted_node1741208368283.writeFrame(Step2JoinsandFilters_node1741208058715)
job.commit()