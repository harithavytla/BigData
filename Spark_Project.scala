package project

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import com.databricks.spark.avro
import com.databricks.spark.xml

case class schema(Direction:String,Year:String,Date:String,Weekday:String,Current_Match:String,Country:String,Commodity:String,Transport_Mode:String,Measure:String,Value:String,Cumulative:String)
object project {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("csv.enable.summary-metadata", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    sc.hadoopConfiguration.set("avro.enable.summary-metadata", "false")
    sc.hadoopConfiguration.set("json.enable.summary-metadata", "false")
    //Reading Data
    val raw_data = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//covid-data.txt")
    val header = raw_data.first()
    //Removing header
    val data = raw_data.filter(x=>x!=header)
    //Raw data row count
    println(raw_data.count())
    //Filtering Tonnes Data
    val tonnes_data = data.map(x=>x.split(","))
                        .map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))
                        .filter(x=>x.Measure=="Tonnes")
    //Converting tonnes data into DF
    val tonnes_df = tonnes_data.toDF()
    //Tonnes data count
    println(tonnes_data.count())
    //Converting to Row RDD with $ filter
    val row_rdd = data.map(x=>x.split(","))
                      .map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))
                      .filter(x=>x(8)=="$")
    // $ filter count
    println(row_rdd.count())
    //Reading schema as RDD and converting to List
    val struct = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//schema.txt")
                        .collect()
                        .toList
    val schema_list = struct.flatMap(x=>x.split(","))
    //Imposing Struct Schema
    val struct_schema = StructType(schema_list.map(x=>StructField(x,StringType,true)))
    //Creating DF from Row RDD
    val $_df = spark.createDataFrame(row_rdd,struct_schema)
    //Registering DF as Temp Tables
    tonnes_df.createOrReplaceTempView("tonnes_df")
    val tonnes_exports = spark.sql("select * from tonnes_df where Direction='Exports'")
    //Write tonnes DF as CSV
    tonnes_exports.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("C:///Users//bnama//Desktop//BigData_local//Data//tonnes_export")
    //$ exports Temp View
    $_df.createOrReplaceTempView("dollar_df")
    val $_exports = spark.sql("select * from dollar_df where Direction='Exports'")
    //Write $ exports as parquet
    $_exports.coalesce(1).write.format("parquet").mode("overwrite").save("C:///Users//bnama//Desktop//BigData_local//Data//$_exports")
    //$ imports as Temp View
    val $_imports = spark.sql("select * from dollar_df where Direction='Imports'")
    //Write $ imports as json
    $_exports.coalesce(1).write.format("json").mode("overwrite").save("C:///Users//bnama//Desktop//BigData_local//Data//$_imports")
    //$ reimports
    val $_reimports=spark.sql("select * from dollar_df where Direction='Reimports'")
    //Write $ imports as json
    $_exports.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("C:///Users//bnama//Desktop//BigData_local//Data//$_reimports")

    //Reading all the 4 file formats as DF
    
    val tonnes_df_read = spark.read.format("csv").option("header","true").load("C:///Users//bnama//Desktop//BigData_local//Data//tonnes_export")
    /*val $_exports_read = spark.read.format("csv").option("header","true").load("C:///Users//bnama//Desktop//BigData_local//Data//$_exports")*/
    val $_imports_read = spark.read.format("csv").option("header","true").load("C:///Users//bnama//Desktop//BigData_local//Data//$_imports")
    /*val $_reimports_read = spark.read.format("csv").option("header","true").load("C:///Users//bnama//Desktop//BigData_local//Data//$_reimports")*/
    val union_data = tonnes_df_read.union($_imports_read)
    //Registering Temp View
    union_data.createOrReplaceTempView("Union")
    //Extracting date and month from Date column
    val final_df = spark.sql("select *,from_unixtime(unix_timestamp(Date,'dd/MM/yyyy'),'yyyy') as year_1,from_unixtime(unix_timestamp(Date,'dd/MM/yyyy'),'MM') as month,from_unixtime(unix_timestamp(Date,'dd/MM/yyyy'),'dd') as date_1 from Union")
    //Writing Union Data as partition into a json file
    final_df.coalesce(1).write.format("json").option("header","true").mode("overwrite").partitionBy("year_1","month","date_1","Direction").save("C:///Users//bnama//Desktop//BigData_local//Data//Union")
    //XML Data
    union_data.coalesce(1).write.format("xml").option("rootTag","Covid_data").option("rowTag","report").save("C:///Users//bnama//Desktop//BigData_local//Data//Union_XML")
  }
}