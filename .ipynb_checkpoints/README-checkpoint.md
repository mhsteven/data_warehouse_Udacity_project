### Purpose
The purpose of this dataset is to provide easier way to explore data including songs and user activity on Sparkify's new music streaming app. Historically, these data are reside in JSON format in a directory, which is not easy to query, summarize, and perform analytical computation. Therefore, our approach here is to build an ETL pipeline to pull these JSON files which stored in S3 bucket into a SQL database. With this final star schema database, users can easily explore the data to understand their users better and more efficiently.  


### Database design
We build an ETL pipeline to pull all JSON log from the directory into Postgres database. We purposely build our database with **Star Schema** to optimize the query analysis since we know the objective of this database is to query for specific user activity log for analytical computation, and are likely to have multiple people access it paired with large amount of existing and new log activity pull in, so the simplicity of star schema is most suitable for this purpose. We have 2 datasets, firstly, the song dataset containing song and artist metadata such as title are artist name, and secondly, the log dataset containing user activities including the timestamp they start to listen, the song they listen to, their gender and location etc. We split our dataset and build 5 tables to be our fact table and dimension tables as follows:
1. Fact table: 
    - songplays
2. Dimension table:
    - users
    - songs
    - artists
    - time

### ETL pipeline
We start with creating a Redshift cluster on AWS to serve as a data warehouse, and assign Redshift with S3 read IAM role so that it can copy data from the specified S3 bucket. We first copy data from S3 without transformation into Redshift as staging tables. Then we create additional 5 tables (as shown above) by joining and copying these staging tables so we can build a star schema which is more tailored for analytics purpose.

### How to use the files for ETL
1. run helper_notebook.ipynb STEP 1 - 2: This set up the Redshift cluster and assign appropriate role, and store necessary parameter for later ETL process.  this create the database and necessary tables following the star schema ready to be inserted.
2. run create_tables.py in terminal: This create necessary table schema in the Redshift cluster. It includes 2 staging tables, and 5 tables to set up star schema. 
3. run etl.py: This contain the automated code for ETL to pull all JSON files from 2 directory in the our specified S3 bucket and copy into our Redshift data warehouse into 2 staging tables. Then it further copy the necessary fields from the staging tables into 5 different tables to form the star schema. 
4. sql_queries.py: (Optional) modify this if you want to further adjust the schema etc.
5. The helper_notebook.ipynb STEP 3 and beyond (Optional): These are to run additional SQL query on the created tables in Redshift for either validation purpose or analytics purpose.

### Example queries and analysis

1. Top 5 most count of songplay by locations
```sql
SELECT location, count(*) 
FROM songplays 
GROUP BY location 
ORDER BY count(*) 
DESC LIMIT 5;
```

| location | count |
| ---------| -------|
|San Francisco-Oakland-Hayward, CA | 168 |
|Lansing-East Lansing, MI | 128 |
|Portland-South Portland, ME | 124 |
|Waterloo-Cedar Falls, IA | 84 |
|Tampa-St. Petersburg-Clearwater, FL | 72 |



2. Count of songplay by gender and by level
```sql
SELECT songplays.level AS user_type, 
    COUNT(CASE WHEN users.gender = 'F' THEN 1 ELSE NULL END) AS Female_count, 
    COUNT(CASE WHEN users.gender = 'M' THEN 1 ELSE NULL END) AS Male_count 
    FROM songplays 
    JOIN users ON songplays.user_id = users.user_id 
    GROUP BY songplays.level;
```

|user_type | female_count | male_count |
| ---------| -------------| ----------|
| free | 1240 | 400 |
| paid | 136 | 132 |



3. Count of songplay by weekday
```sql
SELECT time.weekday, 
    COUNT(songplays.songplay_id) AS number_of_play 
    FROM songplays 
    JOIN time ON songplays.start_time = time.start_time 
    GROUP BY time.weekday 
    ORDER BY weekday;
```

|weekday | number_of_play |
| ---------| -------------|
| 0 | 156 |
| 1 | 220 |
| 2 | 244 |
| 3 | 168 |
| 4 | 180 |
| 5 | 160  |
| 6 | 204  |


