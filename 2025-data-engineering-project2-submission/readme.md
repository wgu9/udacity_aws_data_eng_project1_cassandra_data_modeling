# Sparkify S3 to Redshift ETL Project

## Project Overview
Sparkify, a music streaming startup, is migrating its data processes to the cloud. The goal is to build an ETL pipeline that extracts data from AWS S3, stages it in Amazon Redshift, and transforms it into a star schema for analytical queries.

## Goal
Enable Sparkify's analytics team to gain insights on user behavior and song preferences efficiently using a scalable cloud-based data warehouse.

### Business Context

Sparkify needs this data warehouse to:
1. Understand user listening patterns to improve engagement
2. Analyze song popularity trends for content acquisition
3. Track user subscription changes for revenue optimization
4. Monitor platform usage patterns for capacity planning
5. Enable targeted marketing based on user preferences

## Implementation 

### 1. Data Sources
- **S3 Buckets:**
  - `song_data`: JSON metadata about songs.
  - `log_data`: JSON logs of user activity.
  - `log_json_path`: JSON format configuration for parsing.

### 2. ETL Process
1. **Extract**: Load raw data from S3 into Redshift staging tables.
2. **Transform**: Clean and structure data into facts and dimensions.
3. **Load**: Insert processed data into an analytical star schema.

### 3. Data Warehouse Schema (Star Schema)

![The relations between tables](final/etl_image.png)

I believe the most challenging aspects of this data warehouse project are the three steps below.


#### 3.1 Table Definition and Schema Design
- Understanding what tables are needed
- Determining appropriate data types and constraints
- Designing the schema to support analytical queries


This project utilizes two datasets stored in Amazon S3:
- **Song Data**: `s3://udacity-dend/song_data`
- **Log Data**: `s3://udacity-dend/log_data`
- **Metadata** (for parsing log data): `s3://udacity-dend/log_json_path.json`

The above datasets are structured in JSON format:
- **Song Dataset**: Extracted from the Million Song Dataset, containing song metadata.
- **Log Dataset**: Simulated event logs from a music streaming app.

Next, here are the two tables as data sources to copy from S3 to Redshift Staging Tables:
- `staging_songs` (raw song data)
- `staging_logs` (raw log data)

#### 3.2. Identifying Fact and Dimension Tables
- Recognizing songplays as the fact table (contains measures/events)
- Identifying the supporting dimension tables (users, songs, artists, time)
- Understanding the relationships between above tables and the source tables (two staging tables)

**Fact Table**
- **songplays** - records of song plays
  - `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

**Dimension Tables**
- **users** - user details
  - `user_id`, `first_name`, `last_name`, `gender`, `level`
- **songs** - song details
  - `song_id`, `title`, `artist_id`, `year`, `duration`
- **artists** - artist details
  - `artist_id`, `name`, `location`, `latitude`, `longitude`
- **time** - timestamp details
  - `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

The star schema design was chosen because:
1. It simplifies complex queries for business analysts
2. Enables fast aggregations for reporting
3. Provides flexible dimensions for various analyses
4. Supports efficient joins for performance
5. Makes it easy to add new dimensions


#### 3.3 Writing Effective Queries
- Creating efficient JOIN operations
- Handling timestamp conversions correctly
- Managing data transformations
- Ensuring data quality in INSERT statements

### Execution

### Technologies Used in the Project
- **AWS S3**: Data storage
- **AWS Redshift**: Cloud-based data warehouse
- **Python**: ETL scripting
- **SQL**: Data transformation and querying

### Key Considerations
- Bulk data loading using `COPY` for efficiency.
- Role-based access control for security.
- Optimized schema design for analytical performance.

### Project Files
- `create_table.py` - Defines and creates database tables
- `etl.py` - Extracts, transforms, and loads data into Redshift
- `sql_queries.py` - SQL statements for table creation and data loading
- `README.md` - Documentation of project details
- `count_rows.py` - This script connects to a Redshift cluster, counts the number of rows in specified tables, and prints the row counts. 
- `example_queries.py` - This script connects to a Redshift cluster and runs example queries to demonstrate the data analysis capabilities of the Star Schema.


Run this command below.

```bash
python create_tables.py && python etl.py && python count_rows.py
```

### Results

```
Row counts for all tables:
----------------------------------------
staging_events: 8,056 rows
staging_songs: 385,212 rows
songplays: 6,961 rows
users: 105 rows
songs: 384,955 rows
artists: 45,262 rows
time: 8,023 rows
```

# Appendix. SQL Query Analysis & Optimization

## Appendix 1. CREATE TABLE Queries

### Current Implementation
```sql
CREATE TABLE staging_events (...)
CREATE TABLE staging_songs (...)
CREATE TABLE songplays (...)
CREATE TABLE users (...)
CREATE TABLE songs (...)
CREATE TABLE artists (...)
CREATE TABLE time (...)
```

## Appendix 2. Staging Tables

### 2.1 Create Tables
For staging tables that are just temporary holders for data from S3, we don't need such complex optimizations.

The staging tables serve a simple purpose:
1. Receive raw data from S3 JSON files
2. Hold it temporarily
3. Transform and load it into final tables

Why simpler is better for staging:
1. Staging tables are temporary - data won't stay there long
2. We're not doing complex queries on staging tables
3. The main performance consideration is load speed from S3
4. Adding distribution and sort keys can actually slow down the COPY process


```sql
CREATE TABLE staging_events (
    ...
)
```

Save the optimizations (DISTKEY, SORTKEY, ENCODE) for the final tables where they'll actually benefit query performance.

### 2.2 COPY Queries
My staging COPY commands are appropriate for this use case. They include essential elements:

1. **Parameters**
- JSON parsing options (`json {}` for complex logs, `json 'auto'` for songs)
- Region specification
- IAM role credentials
- Reasonable error tolerance (`maxerror 10000`)

1. **Performance Settings**
- Disabled compression analysis (`compupdate off`)
- Disabled statistics updates (`statupdate off`)
   
These settings are optimal for staging tables because:
- They prioritize fast loading over query optimization
- They handle both structured and semi-structured JSON data
- They provide sufficient error tolerance for large datasets
- They minimize overhead during the loading process

**Current Implementation**
```sql
-- For staging_events
COPY staging_events 
FROM 's3://udacity-dend/log-data'
credentials 'aws_iam_role={}'
json {} -  -- Uses external JSON path file
region 'us-west-2'
maxerror 10000 -- Reasonable for large datasets
compupdate off  -- Keep off for staging
statupdate off;  -- Keep off for staging

-- For staging_songs
COPY staging_songs 
FROM 's3://udacity-dend/song-data'
credentials 'aws_iam_role={}'
json 'auto' -   -- Automatically detects JSON format
region 'us-west-2'
maxerror 10000
compupdate off
statupdate off;
```

The only minor suggestion would be adding `TRUNCATECOLUMNS` to handle potential oversized values, but this is optional.

## Appendix 3. Final Tables and Their Optimizations

### 3.1. Fact Table: songplays
```sql
CREATE TABLE songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
)
DISTSTYLE KEY
DISTKEY(user_id)      -- Optimize for joins with users table
SORTKEY(start_time);  -- Most queries filter by time
```
Why these choices:
- DISTKEY on user_id because we frequently join with users table
- SORTKEY on start_time for time-based queries
- Most analytics will involve time-series analysis

The compound sort key `COMPOUND SORTKEY(start_time, user_id)` would only be better if we frequently run queries that filter on BOTH time AND user_id in that specific order. Since time-based analysis is likely the primary use case, the single SORTKEY is the better choice.

### 3.2. Dimension Tables

#### users
```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
)
DISTSTYLE ALL
SORTKEY (user_id);
```

Why:
1. Combines benefits of both approaches
2. DISTSTYLE ALL because:
   - Users table is small (dimension table)
   - Frequently joined with songplays
   - Reduces network traffic during joins
3. SORTKEY (user_id) because:
   - Primary key lookups are common
   - Often used in WHERE clauses
   - Improves query performance for user-specific queries

This combination provides optimal performance for:
- JOIN operations with songplays
- User lookup queries
- Range scans on user_id
- Overall query performance

#### songs
```sql
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration FLOAT
)
DISTSTYLE ALL
SORTKEY(song_id);
```

Why?
1. Songs table is a dimension table, likely to be:
   - Relatively small compared to fact table
   - Frequently joined with songplays
   - Not updated frequently

2. DISTSTYLE ALL is better than DISTSTYLE KEY here because:
   - Eliminates data redistribution during joins
   - Reduces network traffic
   - Improves join performance with songplays table

3. SORTKEY(song_id) is still useful for:
   - Quick lookups by song_id
   - Efficient joins
   - Range scans if needed

#### artists
```sql
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
DISTSTYLE ALL
SORTKEY (artist_id);
```

Why This is Better:

1. **DISTSTYLE ALL** because:
   - Artists is a classic dimension table
   - Relatively small number of records
   - Frequently joined with songplays table
   - Static data that doesn't change often
   - Replication to all nodes improves join performance

2. **SORTKEY (artist_id)** because:
   - Common lookup key
   - Used in joins with songplays
   - Efficient for artist-specific queries

#### time
```sql
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
DISTSTYLE KEY
DISTKEY(start_time)
SORTKEY(start_time);
```
Why:
- Used in time-based analytics
- Frequently joined with songplays
- Sorted by time for range queries

### Overall Recommendation

1. **Distribution Strategy**
   - Use DISTKEY for join-heavy tables
   - Use DISTSTYLE ALL for small dimension tables
   - Use EVEN distribution for staging tables

2. **Sort Keys**
   - Implement COMPOUND SORTKEY for predictable query patterns
   - Use INTERLEAVED SORTKEY for varied query patterns

3. **Data Loading**
   - Enable compression
   - Use appropriate error handling
   - Implement proper data validation

4. **Query Performance**
   - Use DISTINCT for deduplication
   - Add appropriate WHERE clauses
   - Optimize JOIN conditions

## Appendix 4. Data Analysis Results

To validate the effectiveness of our star schema design and demonstrate its analytical capabilities, I ran several business-focused queries that provided valuable insights into user behavior and platform usage.

### Key Findings

1. **Top 5 Most Popular Songs**
   - "Greece 2000" (55 plays)
   - "You're The One" (37 plays)
   - "Stronger" (28 plays)
   - "Revelry" (27 plays)
   - "Yellow" (24 plays)

2. **Platform Usage Patterns**
   Peak activity hours show a clear afternoon/evening trend:
   - 4:00 PM: 575 plays (highest activity)
   - 5:00 PM: 515 plays
   - 6:00 PM: 502 plays
   - 3:00 PM: 473 plays
   - 2:00 PM: 445 plays

3. **User Subscription Analysis**
   - Paid Users: 3,711 plays (53.3%)
   - Free Users: 3,250 plays (46.7%)

### Business Implications

1. **Content Strategy**
   - Clear user preference for certain songs suggests opportunities for similar content acquisition
   - Top songs data can inform playlist curation and recommendation systems

2. **Platform Operations**
   - Peak usage during 2-6 PM indicates optimal times for:
     * Feature releases
     * System maintenance windows
     * Customer support staffing
     * Marketing campaign timing

3. **Monetization**
   - Slightly higher activity from paid users (53.3%) indicates:
     * Successful premium feature adoption
     * Potential for converting more free users
     * Healthy revenue generation potential


1. **User Activity Analysis**
```sql
SELECT u.level, COUNT(*)
FROM songplays s
JOIN users u ON s.user_id = u.user_id
WHERE s.start_time BETWEEN ... AND ...
GROUP BY u.level;
```

2. **Popular Songs Analysis**
```sql
SELECT s.title, COUNT(*)
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY COUNT(*) DESC;
```

3. **Time-based Analysis**
```sql
SELECT t.hour, COUNT(*)
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY t.hour;
```




## Appendix 5. Key Principles Behind These Choices:

1. **Distribution Strategy**
   - Small tables (users, artists) → DISTSTYLE ALL
   - Large tables with joins → DISTSTYLE KEY
   - Optimize for common join patterns

2. **Sort Keys**
   - Based on WHERE clause patterns
   - Based on range scans
   - Based on ORDER BY usage

3. **Primary Keys and Foreign Keys**
   - Enforce data integrity
   - Aid query optimization
   - Support referential integrity

4. **Column Ordering**
   - Most frequently used columns first
   - Helps with compression
   - Improves query performance

These optimizations ensure efficient:
- JOIN operations
- WHERE clause filtering
- Aggregation queries
- Time-series analysis
- User behavior analysis
