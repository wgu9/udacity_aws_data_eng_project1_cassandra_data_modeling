# Project Submission - list of files

```bash
aws s3 cp ./project_mywork/starter/customer/landing/customer-1691348231425.json s3://jeremy-udacity-spark/customer/landing/ --profile myaws2
aws s3 cp ./project_mywork/starter/accelerometer s3://jeremy-udacity-spark/accelerometer/ --recursive --profile myaws2
aws s3 cp ./project_mywork/starter/step_trainer s3://jeremy-udacity-spark/step_trainer/ --recursive --profile myaws2
```


## Expected Files
Python Files:

- [x] [customer_landing_to_trusted.py](./job2-AWS%20Glue%20Jobs/customer_landing_to_trusted.py)
- [x] [accelerometer_landing_to_trusted.py](./job2-AWS%20Glue%20Jobs/accelerometer_landing_to_trusted.py)
- [x] [customer_trusted_to_curated.py](./job3-AWS%20Glue%20Cust%20Curated%20482/customer_trusted_to_curated.py)
- [x] [step_trainer_trusted.py](./job4-AWS%20Glue%20trainer%20and%20machine/step_trainer_trusted.py)
- [x] [machine_learning_curated.py](./job4-AWS%20Glue%20trainer%20and%20machine/machine_learning_curated.py)

SQL DDL Scripts:

- [x] [customer_landing.sql](./job1-sql/customer_landing.sql)
- [x] [accelerometer_landing.sql](./job1-sql/accelerometer_landing.sql)
- [x] [step_trainer_landing.sql](./job1-sql/step_trainer_landing.sql)

Screenshots (from Athena queries):

Landing Zone:
- [x] Customer: 956 rows [customer_landing.png](./job1-sql/customer_landing.png)
- [x] Accelerometer: 81,273 rows [accelerometer_landing.png](./job1-sql/accelerometer_landing.png)
- [x] Step Trainer: 28,680 rows [step_trainer_landing.png](./job1-sql/step_trainer_landing.png)

Trusted Zone:
- [x] Customer: 482 rows [customer_trusted.png](./job2-AWS%20Glue%20Jobs/customer_trusted.png)
- [x] Step Trainer: 14,460 rows [step_trainer_trusted.png](./job4-AWS%20Glue%20trainer%20and%20machine/step_trainer_trusted.png)
- [x] Accelerometer: 40,981 rows [accelerometer_trusted.png](./job2-AWS%20Glue%20Jobs/accelerometer_trusted.png)

Curated Zone:
- [x] Machine Learning: 43,681 rows [machine_learning_curated.png](./job4-AWS%20Glue%20trainer%20and%20machine/machine_learning_curated.png)
- [x] Customer: 482 rows [customers_curated.png](./job3-AWS%20Glue%20Cust%20Curated%20482/customers_curated.png)

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