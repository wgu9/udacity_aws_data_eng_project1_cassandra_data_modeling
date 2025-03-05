### Data Sources
1. **Customer data** from website (customer_landing)
2. **Accelerometer data** from mobile app (accelerometer_landing)
3. **Step Trainer data** from IoT devices (step_trainer_landing)


```bash
aws s3 cp ./project_mywork/starter/customer/landing/customer-1691348231425.json s3://jeremy-udacity-spark/customer/landing/ --profile myaws2
aws s3 cp ./project_mywork/starter/accelerometer s3://jeremy-udacity-spark/accelerometer/ --recursive --profile myaws2
aws s3 cp ./project_mywork/starter/step_trainer s3://jeremy-udacity-spark/step_trainer/ --recursive --profile myaws2
```

### Required Tasks

1. **Landing Zone Setup**:
   - Create S3 directories for the three landing zones
   - Create Glue tables for initial data exploration
   - Submit SQL scripts: customer_landing.sql, accelerometer_landing.sql, step_trainer_landing.sql
   - Provide screenshots of Athena queries showing the data

2. **Customer Data Sanitization (Landing → Trusted)**:
   - Create a Glue job to filter customer records
   - Keep only customers who agreed to share data for research
   - Create `customer_trusted` table
   - Provide screenshot of Athena query showing the results

3. **Accelerometer Data Sanitization (Landing → Trusted)**:
   - Create a Glue job to filter accelerometer readings
   - Keep only readings from customers who agreed to share data
   - Create `accelerometer_trusted` table

4. **Customer Data Curation (Trusted → Curated)**:
   - Create a Glue job to further filter customer data
   - Keep only customers who have accelerometer data and agreed to share
   - Create `customers_curated` table

5. **Step Trainer Data Processing (Landing → Trusted)**:
   - Create a Glue job to process Step Trainer IoT data
   - Link with customers who agreed to share data
   - Create `step_trainer_trusted` table

6. **Machine Learning Dataset Creation (Curated)**:
   - Create a Glue job to aggregate step trainer and accelerometer data
   - Match by timestamp for agreed customers
   - Create `machine_learning_curated` table

## Expected Files

customer_landing_to_trusted.py
accelerometer_landing_to_trusted.py
customer_trusted_to_curated.py
step_trainer_trusted.py (or step_trainer_landing_to_trusted.py)
machine_learning_curated.py


Additional SQL Files:

- [x] customer_landing.sql
- [x] accelerometer_landing.sql
- [x] step_trainer_landing.sql

Additional Screenshots:

- [x] customer_landing.png/jpeg
- [x] accelerometer_landing.png/jpeg
- [x] step_trainer_landing.png/jpeg
- [x] customer_trusted.png/jpeg

## Expected Row Counts

Landing Zone:

- [x] Customer: 956 rows x
- [x] Accelerometer: 81,273 rows x
- [x] Step Trainer: 28,680 rows x


Trusted Zone:

- [x] Customer: 482 rows x
- [x] Accelerometer: 40,981 rows x
- [x] Step Trainer: 14,460 rows x 


Curated Zone:

- [x] Customer: 482 rows x 
- [x] Machine Learning: 43,681 rows x