// Databricks notebook source
// MAGIC %md
// MAGIC # Basic Transformations
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Learn to use basic aggregations.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Get Test Data
// MAGIC Mount the training data set for Databricks S3 bucket by executing the following cell.

// COMMAND ----------

if (!dbutils.fs.mounts.map(mnt => mnt.mountPoint).contains("/mnt/training"))
  dbutils.fs.mount(
    source = "s3a://AKIAJBRYNXGHORDHZB4A:a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF@databricks-corp-training/common",
    mountPoint = "/mnt/training")    

println("Data from s3 is ready to use")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Basic Transformations
// MAGIC 
// MAGIC Using <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">built-in Spark functions</a>, you can aggregate data in various ways. 
// MAGIC 
// MAGIC Run the cell below to compute the average of all salaries in the people DataFrame.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> By default, you get a floating point value.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC %md
// MAGIC <iframe  
// MAGIC src="//fast.wistia.net/embed/iframe/44ui5pgc61?videoFoam=true"
// MAGIC style="border:1px solid #1cb1c2;"
// MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
// MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
// MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
// MAGIC <div>
// MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/44ui5pgc61?seo=false">
// MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
// MAGIC </div>

// COMMAND ----------

val peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet").cache()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Functions on dataframes
// MAGIC 
// MAGIC There are some functions that you can invoke directly on dataframes.  You can find a more complete list on the official docs.  We've seen the use of .show() already, but functions like .printSchema(), .count() and .head(5) are very useful to get oriented with your data.

// COMMAND ----------

peopleDF.printSchema

// COMMAND ----------

peopleDF.count()

// COMMAND ----------

peopleDF.head(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Databricks has a built in function "display()" that can also help you visualize your dataframe.

// COMMAND ----------

display(peopleDF)

//Click on the graph icon below and play around with different displays of the data.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Importing Functions
// MAGIC Some functions operate on columns.  For these you need to use the select on your dataframe to get access to the columns.
// MAGIC Reference the Spark docs for more details and functions.
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package
// MAGIC 
// MAGIC Note the import function in the next cell.
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.avg
// MAGIC 
// MAGIC This is a common place for errors.  Though spark has some functions out of the box, most of the transformation functions need to be imported.  It's common to see the import statement written with the following Scala implicits syntax:
// MAGIC import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.functions.avg
val avgSalaryDF = peopleDF.select(avg($"salary") as "averageSalary")

// COMMAND ----------

avgSalaryDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Convert that value to an integer using the `round()` function. See
// MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" class="text-info">the documentation for <tt>round()</tt></a>
// MAGIC for more details.

// COMMAND ----------

// IMPORT FUNCTIONS HERE
val roundedAvgSalaryDF = // FILL IN 

roundedAvgSalaryDF.show()

// COMMAND ----------


lazy val results = roundedAvgSalaryDF.collect()

assert(Row(72633.0) == results(0), "The average salary is not correct")

println("Congratulations, the average salary computation is correct!")


// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the rounded average salary, what are the minimum and maximum salaries?

// COMMAND ----------

val salaryDF = // FILL IN

salaryDF.show()

// COMMAND ----------

lazy val results = salaryDF.collect()

assert(Row(72633.0,-26884,180841) == results(0), "The average salary is not correct")

println("Congratulations, the average, minimum and maximum salary computations are correct!")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##GroupBy
// MAGIC Notice in the spark documentation that groupby does not return a dataset, but a RelationalGroupedDataset.  This means that you cannot chain dataset operations on it.  
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
// MAGIC 
// MAGIC If you look up RelationalGroupedDataset in the Spark docs you will see that you can use the agg function (along with some other functions) which will return a DataFrame.
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC In the following example we calculate the average salary by gender.

// COMMAND ----------

val salaryByGenderDF = peopleDF.groupBy($"gender").agg(avg($"salary"))

salaryByGenderDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now it's your turn, calculate the rounded average salary by firstname, and order by the average salary in decending order.

// COMMAND ----------

val avgSalaryByName = // FILL IN

avgSalaryByName.show()

// COMMAND ----------

lazy val results = avgSalaryByName.collect()

assert(Row("Sherry",74424.0) == results(0), "The average salary by name is not correct")
assert(Row("Remona",74312.0) == results(1), "The average salary by name is not correct")
assert(Row("Dessie",74309.0) == results(2), "The average salary by name is not correct")


println("Congratulations, the average salary by name calculation is correct!")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now for a little more challenge and some practice with dates. Calculate the rounded average salary by birthyear and order by birthyear in asending order.

// COMMAND ----------

val avgSalaryByBirthYear = // FILL IN

avgSalaryByBirthYear.show()

// COMMAND ----------

lazy val results = avgSalaryByBirthYear.collect()

assert(Row(1951,72704.0) == results(0), "The average salary by birth year is not correct")
assert(Row(1952,72614.0) == results(1), "The average salary by birth year is not correct")
assert(Row(1953,72659.0) == results(2), "The average salary by birth year is not correct")

println("Congratulations, the average salary by birth year computations are correct!")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ###Adding columns
// MAGIC The .withColumnRenamed() function that operates on a dataframe is helpful to create new dataframes with additional columns.
// MAGIC 
// MAGIC Here is an example of that analyzes the average salary based on the length of a person's last name.

// COMMAND ----------

val lastNameLengthAvgSalaryDF = peopleDF
    .withColumn("lastNameLength", length($"lastName"))
    .groupBy($"lastNameLength")
    .agg(avg($"salary"))
    .orderBy($"lastNameLength")

// COMMAND ----------

lastNameLengthAvgSalaryDF.show()

// COMMAND ----------

// MAGIC %md Create a dataframe with the data from people df plus three new columns: birth year, birth month (1 to 12), and birth day (1 to 31).

// COMMAND ----------

// FILL IN

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Explore the Data
// MAGIC 
// MAGIC Let's explore the peopleDF data by creating a table with only the columns that are used to identify names.

// COMMAND ----------

val namesDF = peopleDF
    .drop("id")
    .drop("ssn")
    .drop("birthDate")
    .drop("salary")
    .drop("gender")
    .cache()

namesDF.count()

// COMMAND ----------

// MAGIC %md Why there are 10,000,000 names? Does everyone have a different name?

// COMMAND ----------

// ADD YOUR ANALYSIS HERE
// dataframes do not need to have unique rows... this is a key difference between a relational table and a dataframe.  


// COMMAND ----------

// MAGIC %md
// MAGIC ### Explore on your own
// MAGIC 
// MAGIC That concludes the basic transformations introduction.  Feel free to explore the data in peopleDF further.  What other insights can you get from this data?
// MAGIC 
// MAGIC (Also, take a look at the jobs, stages, tasks and DAGs produced by the transformations.)
