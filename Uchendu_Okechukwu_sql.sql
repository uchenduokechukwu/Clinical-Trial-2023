-- Databricks notebook source
-- MAGIC %python
-- MAGIC myrdd1 = sc.textFile('/FileStore/tables/clinicaltrial_2023')
-- MAGIC myrdd1.take(2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd2 = myrdd1.map(lambda line: line)
-- MAGIC myrdd2.take(9)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd3 = myrdd2.map(lambda line: line.split('\t'))
-- MAGIC myrdd3.take(9)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC first_row_length = len(myrdd3.first())
-- MAGIC
-- MAGIC myrdd_clean = myrdd3.filter(lambda row: len(row) == first_row_length)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC header = myrdd_clean.first()
-- MAGIC
-- MAGIC # Filter out the header
-- MAGIC myrdd_no_header = myrdd_clean.filter(lambda x: x != header)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Convert RDD to DataFrame
-- MAGIC clinicaltrial_2023 = spark.createDataFrame(myrdd_no_header, ['Id','Study_Title','Acronym', 'Status', 'Conditions', 'Interventions', 'Sponsor', 'Collaborators', 'Enrollment', 'Funder Type', 'Type', 'Study Design', 'Start', 'Completion'])
-- MAGIC
-- MAGIC # Show DataFrame
-- MAGIC clinicaltrial_2023.show(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import regexp_replace
-- MAGIC df = clinicaltrial_2023.select('Study_Title', 'Status','Conditions','Sponsor','Type', 'Completion')
-- MAGIC clinicaltrial_2023_cleaned = df.withColumn('Completion', regexp_replace(regexp_replace('Completion',',', ''),'"',''))
-- MAGIC clinicaltrial_2023_cleaned.show(6)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd4 = sc.textFile('/FileStore/tables/pharma')
-- MAGIC myrdd4.take(3) 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd5 = myrdd4.map(lambda line: line.replace('"', ''))
-- MAGIC myrdd5.take(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd6 = myrdd5.map(lambda line: line.split(','))
-- MAGIC myrdd6.take(3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC myrdd7 = myrdd6.map(lambda x:x[1])
-- MAGIC myrdd7.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharmheader = myrdd7.first()
-- MAGIC pharm_no_header = myrdd7.filter(lambda x: x != pharmheader)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import StringType, StructType, StructField
-- MAGIC
-- MAGIC schema = StructType([StructField("Parent_Company", StringType(), True)])
-- MAGIC # Convert RDD to DataFrame
-- MAGIC parentcompanydf = spark.createDataFrame(pharm_no_header.map(lambda x: (x,)), schema)
-- MAGIC
-- MAGIC # Show DataFrame
-- MAGIC parentcompanydf.show(7)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023_cleaned.createOrReplaceTempView('clinicaltrial2023')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC parentcompanydf.createOrReplaceTempView('ParentCompany')

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from clinicaltrial2023 limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #1. Number of total and Distinct studies

-- COMMAND ----------

select count(distinct Study_Title) as Distinct_number_of_studies, count(Study_Title ) as Total_number_of_studies from clinicaltrial2023

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #2. Type of Studies with their frequencies

-- COMMAND ----------

select Type, count(*) as frequency from clinicaltrial2023
where Type not in ('', ' ')
 group by Type
 order by frequency desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #3. Top 5 conditions with their frequencies

-- COMMAND ----------

with seperated_condition as (select explode(split(Conditions, '\\|')) as Condition from clinicaltrial2023) 
select Condition, count(*) as Frequency from seperated_condition group by Condition order by count(*) desc limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #4. 10 most common sponsors that are not pharmaceutical companies with the number of trials they have sponsored

-- COMMAND ----------

SELECT s.sponsor, COUNT(*) as Number_of_trials
FROM clinicaltrial2023 s
WHERE NOT EXISTS (
    SELECT 1
    FROM ParentCompany p
    WHERE s.sponsor = p.Parent_Company
)
GROUP BY s.sponsor
order by count(*) desc limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #5. Number of completed studies for each month in 2023.

-- COMMAND ----------

select Completion, status from clinicaltrial2023
where status = 'COMPLETED' and completion like '2023%'
    

-- COMMAND ----------

SELECT 
    SUBSTRING(Completion, 6, 2) AS Month,
    count(*) as completed_studies
FROM clinicaltrial2023
    
where status = 'COMPLETED' and Completion like '2023%'
group by SUBSTRING(Completion, 6, 2)
order by SUBSTRING(Completion, 6, 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Further analysis, 10 most common sponsors that are pharmaceutical companies with the number of trials they have sponsored

-- COMMAND ----------

SELECT s.sponsor, COUNT(*) as Number_of_trials
FROM clinicaltrial2023 s
WHERE s.sponsor IN (
    SELECT p.Parent_Company
    FROM ParentCompany p
    WHERE s.sponsor = p.Parent_Company
)
GROUP BY s.sponsor
order by count(*) desc limit 10
