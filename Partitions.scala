package Partitions

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

object Partitions {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    
    val struct_schema = StructType(StructField("txnno",IntegerType,true)::
                                   StructField("txndate",StringType,true)::
                                   StructField("custno",StringType,true)::
                                   StructField("amount",StringType,true)::
                                   StructField("category",StringType,true)::
                                   StructField("product",StringType,true)::
                                   StructField("city",StringType,true)::
                                   StructField("state",StringType,true)::
                                   StructField("spendby",StringType,true)::Nil)
    val data = spark.read.format("csv").schema(struct_schema).load("C:///Users//bnama//Desktop//BigData_local//Data//txns")
    data.createOrReplaceTempView("Temp")
    val data_df = spark.sql("select * from Temp")
    data_df.show(10)
    data_df.write.format("csv").option("header","true").partitionBy("category","spendby").mode("overwrite").save("C:///Users//bnama//Desktop//BigData_local//Data//txns_partitions")
    
  }
  
}