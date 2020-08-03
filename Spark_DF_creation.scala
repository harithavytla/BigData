package RDD_DF_Seamless

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object RDD_DF_Seamless {
  
  case class txn_schema(txnno : Int, txndate : String,custno : String,amount  : String,category: String,product :String,city :String,state: String,spendby: String)
  
  def main(args : Array[String]) : Unit ={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val raw_data = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//txns")
    
    /* RDD String to DF */
    
    println("RDD String")
    val RDD_string_df = raw_data.map(x=>x.split(","))
                                .map(x=>txn_schema(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
                                .toDF() 
    RDD_string_df.createOrReplaceTempView("Temp_View_RDD_String")
    val result_RDD_String = spark.sql("select * from Temp_View_RDD_String") 
    
    result_RDD_String.show(10)
    
    
    /* RDD Row to DF */
                                
    val struct_schema = StructType(StructField("txnno",IntegerType,true)::
                                   StructField("txndate",StringType,true)::
                                   StructField("custno",StringType,true)::
                                   StructField("amount",StringType,true)::
                                   StructField("category",StringType,true)::
                                   StructField("product",StringType,true)::
                                   StructField("city",StringType,true)::
                                   StructField("state",StringType,true)::
                                   StructField("spendby",StringType,true)::Nil)
                                   
    val RDD_row = raw_data.map(x=>x.split(","))
                          .map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
                                   
    println("RDD Row")
    
    val RDD_row_df = spark.createDataFrame(RDD_row, struct_schema)
    RDD_row_df.createOrReplaceTempView("Temp_View_RDD_Row")
    val result_RDD_row = spark.sql("select * from Temp_View_RDD_Row")
    
    result_RDD_row.show();
    
    /*Seamless Read*/
    
    println("Seamless")
    
    val raw_data_df = spark.read.format("csv").schema(struct_schema).load("C:///Users//bnama//Desktop//BigData_local//Data//txns")
    raw_data_df.createOrReplaceTempView("Temp_View_seamless")
    val result_seamless = spark.sql("select * from Temp_View_seamless")
    
    result_seamless.show(10)
    
    println("Union")
    
    val result = result_RDD_String.union(result_RDD_row).union(result_seamless)
    result.show()
    println(result.count())
  }
}