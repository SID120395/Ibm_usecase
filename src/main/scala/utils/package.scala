import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.ibm.ibmos2spark.CloudObjectStorage

package object utils {
  /*
  utility package consist of methods to read from a bucket(IBM CloudObjectStorage) ,CSV file, DB2.
  utility package consist of methods to write to a bucket(CloudObjectStorage), DB2
  */

  val spark = SparkSession
    .builder()
    .appName("Connect IBM COS")
    .master("local")
    .getOrCreate()

  def read_csv(): DataFrame = {
    /*
    Method to read from a local file system and perform some preprocessing
     */
    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/sidhartha.panigrahi/Downloads/emp-data.csv")
    val filterCond = df.columns.map(x=>col(x).isNotNull).reduce(_ && _) // removes NULL values
    val filteredDf = df.filter(filterCond)
    val df_filter = filteredDf
      .withColumn("Salary_f", functions.regexp_replace(filteredDf.col("Salary"), "[^A-Z0-9_]", ""))
      .drop("Salary")  //remove "$" symbol from the salary data
    val df_cast = df_filter
      .withColumn("Salary_cast", df_filter.col("Salary_f").cast(IntegerType))
      .withColumnRenamed("Salary_cast","Salary")
      .drop("Salary_f")
    return df_cast
  }

  def avg_dept_sal(df :DataFrame): DataFrame = {
    /*
    Method to calculate Average Salary in each Department
     */
    val avg_salary = df.
      groupBy("department").agg(round(avg("salary"),2).as("avg_salary"))
    return avg_salary
  }

  def dept_sal_diff(df :DataFrame): DataFrame = {
    /*
    Method to calculate Salary Differnce between Male and Female in each Department
    Negative value of Salary_diff implies the salary of Female is higher then male
     */
    val gender_salary_gap = df.
      groupBy("department").pivot("Gender").agg(sum("salary"))
      .withColumn("Sal_diff",(col("Male") - col("Female")))
    return gender_salary_gap
  }

  def dept_gen_ratio(df :DataFrame): DataFrame = {
    /*
     Method to calculate Gender Ration in each Department
     */
    val gender_ratio = df.
      groupBy("department").pivot("Gender").agg(count("Gender"))
      .withColumn("total_member",(col("Male") + col("Female")))
      .withColumn("Male_Ratio",round((((col("Male") / col("total_member")) * 100)),2))
      .withColumn("Female_Ratio",round((((col("Female") / col("total_member")) * 100)),2))
   return gender_ratio
  }

  def read_bucket_ibmcloud(): DataFrame = {
    /*
    Method to create a connection between spark and IBM Cloud services to read data from a bucket
     */
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.mycos.access.key", "0aba66146f3b450cacebaa908046d17e")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.mycos.secret.key", "27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.mycos.endpoint", "https://s3.us.cloud-object-storage.appdomain.cloud")
    val cloud_read = spark.
      read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").
      option("header", "true").
      option("inferSchema", "true").
      load("cos://candidate-exercise.myCos/emp-data.csv")
    cloud_read.printSchema()
    cloud_read.show(5, 0)
    return cloud_read
  }

  def write_bucket_ibmcloud(df :DataFrame) = {
    /*
    Method to create a connection between spark and IBM Cloud services to write data from a bucket
     */
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    spark.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.mycos.access.key", "0aba66146f3b450cacebaa908046d17e")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.mycos.secret.key", "27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd")
    spark.sparkContext.hadoopConfiguration.set("fs.cos.mycos.endpoint", "https://s3.us.cloud-object-storage.appdomain.cloud")
    val itemName = "cos://mybucket.service/test.parquet/"
    df.coalesce(1).write.format("parquet").save(itemName)
  }

  def read_db2_ibmcloud(): DataFrame = {
    /*
    Method to create a connection between spark and DB2 services to read data from a table
     */
    val db2_df = spark.read.format("jdbc")
      .option("url", "jdbc:db2://dashdb-txn-sbox-yp-lon02-15.services.eu-gb.bluemix.net:50000/BLUDB")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "table") //unable to create table
      .option("user", "xnk60050")
      .option("password", "bgn5810m+gg720g7")
      .load()
    return db2_df
  }

  def write_db2_ibmcloud(df :DataFrame) = {
    /*
    Method to create a connection between spark and DB2 services to write data to a table
     */
      df.write.format("jdbc")
      .option("url", "jdbc:db2://dashdb-txn-sbox-yp-lon02-15.services.eu-gb.bluemix.net:50000/BLUDB")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "table_name") //unable to create table
      .option("user", "xnk60050")
      .option("","")
      .mode("Append")
      .save()
  }
}
