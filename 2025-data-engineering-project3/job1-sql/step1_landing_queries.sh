#!/bin/bash

# Create the customer_landing.sql file
cat > customer_landing.sql << 'EOF'
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customername` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `serialnumber` string,
  `registrationdate` bigint,
  `lastupdatedate` bigint,
  `sharewithresearchasofdate` bigint,
  `sharewithpublicasofdate` bigint,
  `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://jeremy-udacity-spark/customer/landing/'
TBLPROPERTIES ('classification' = 'json');
EOF

# Create the accelerometer_landing.sql file
cat > accelerometer_landing.sql << 'EOF'
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing` (
  `user` string,
  `timestamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://jeremy-udacity-spark/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
EOF

# Create the step_trainer_landing.sql file
cat > step_trainer_landing.sql << 'EOF'
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing` (
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://jeremy-udacity-spark/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
EOF

echo "SQL files created successfully: customer_landing.sql, accelerometer_landing.sql, step_trainer_landing.sql"