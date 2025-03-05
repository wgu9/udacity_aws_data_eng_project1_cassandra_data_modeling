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

# Script generated for node customer_curated
customer_curated_node1741214523111 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customer_curated_node1741214523111")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1741214363257 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1741214363257")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1741214328019 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1741214328019")

# Script generated for node SQL Query
SqlQuery2519 = '''
SELECT 
    st.sensorReadingTime,
    st.serialNumber,
    st.distanceFromObject,
    ar.timeStamp,
    ar.x,
    ar.y,
    ar.z
FROM step_trainer_trusted st
JOIN accelerometer_trusted ar
    ON st.sensorReadingTime = ar.timeStamp
JOIN customer_curated c
    ON ar.user = c.email;
'''
SQLQuery_node1741214398996 = sparkSqlQuery(glueContext, query = SqlQuery2519, mapping = {"accelerometer_trusted":accelerometer_trusted_node1741214328019, "step_trainer_trusted":step_trainer_trusted_node1741214363257, "customer_curated":customer_curated_node1741214523111}, transformation_ctx = "SQLQuery_node1741214398996")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1741214398996, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741212082148", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1741214568868 = glueContext.getSink(path="s3://jeremy-udacity-spark/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1741214568868")
machine_learning_curated_node1741214568868.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1741214568868.setFormat("json")
machine_learning_curated_node1741214568868.writeFrame(SQLQuery_node1741214398996)
job.commit()