package Seamless_File_Formats

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro
import com.databricks.spark.xml

object Seamless_File_Formats {
  
  def main(args:Array[String]) : Unit = {
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
   val data = spark.read.format("csv").option("header","true").option("inferSchema","true")load("C://Users//bnama//Desktop//BigData_local//Data//usdata.csv")
                          
    data.printSchema()
    
    data.createOrReplaceTempView("Temp")
    
    val age_data = spark.sql("select * from Temp where age < 50")
    
    println("json")
    
    age_data.write.format("json").save("C://Users//bnama//Desktop//BigData_local//Data//usdata_json")
  
    println("csv")
   
    age_data.write.format("csv").option("header","true").save("C://Users//bnama//Desktop//BigData_local//Data//usdata_csv")
    
    println("avro")
    
    age_data.write.format("com.databricks.spark.avro").save("C://Users//bnama//Desktop//BigData_local//Data//usdata_avro")

    println("Parquet")
    
    age_data.write.format("parquet").save("C://Users//bnama//Desktop//BigData_local//Data//usdata_parquet")
    
    println("ORC")
    
    age_data.write.format("orc").save("C://Users//bnama//Desktop//BigData_local//Data//usdata_orc")
    
    println("XML")
    
    val data_xml = spark.read.format("xml").option("rowTag","book")load("C://Users//bnama//Desktop//BigData_local//Data//books.xml")

    data_xml.write.format("csv").option("header","true").save("C://Users//bnama//Desktop//BigData_local//Data//books_csv")
  }
    
  
}