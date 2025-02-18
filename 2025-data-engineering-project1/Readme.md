# Project 1. 

Course **Data Engineering with AWS**

### **Executive Summary**

This project focuses on designing an Apache Cassandra database to analyze user activity data from a music streaming app. The goal was to create efficient data models and execute specific queries to answer key business questions, such as identifying songs played during a session, retrieving user-specific listening history, and finding users who listened to a particular song.

#### **Design Approach**
The project involved two main components: 
1. **ETL Pipeline**: Preprocessing raw CSV files into a consolidated dataset (`event_datafile_new.csv`) for use in Cassandra.
2. **Data Modeling**: Designing three tables tailored to the query requirements, with careful consideration of partition keys and clustering columns to ensure efficient data retrieval without using `ALLOW FILTERING`.

#### **Key Learnings**
- **Cassandra Data Modeling**: I learned how to design tables based on query patterns, leveraging partition keys for data distribution and clustering columns for sorting within partitions.
- **Primary Key Importance**: The combination of partition and clustering keys is critical for ensuring unique row identification and optimizing query performance.
- **Practical Application**: Hands-on experience with Python and Cassandra APIs reinforced my understanding of distributed databases and their role in handling large-scale data efficiently.

#### **Reflection**
This project highlighted the importance of aligning database schema with query needs. It also emphasized the value of clean, modular code and thorough testing to ensure accurate results. Overall, I gained practical skills in ETL processing, data modeling, and working with NoSQL databases, which are essential for real-world data engineering tasks. 

This experience has deepened my appreciation for designing scalable systems that can handle complex analytical queries with high performance.

### Appendix. Installing Packages 
```
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Create requirements.txt with necessary packages
cat > requirements.txt << EOL
pandas
cassandra-driver
numpy
EOL

# Install requirements
pip install -r requirements.txt
```