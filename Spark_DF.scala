package RDD_DF_Seamless

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object RDD_DF_Seamless {
  
  def main(args : Array[String]) : Unit ={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    
    val raw_data = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//usdata.csv")
    val header = raw_data.first()
    val data_without_header = raw_data.filter(x=>x!=header)
    
    val struct_schema = StructType(StructField("first_name1",StringType,true)::
                                   StructField("email1",StringType,true)::Nil)
                                   
    val data = data_without_header.map(x=>x.split(","))
                                  .map(x=>Row(x(0),x(12)))
    val df = spark.createDataFrame(data, struct_schema)
    df.createOrReplaceTempView("Temp_View")
    val result = spark.sql("select * from Temp_View")
    
    result.show();
    
  }
}