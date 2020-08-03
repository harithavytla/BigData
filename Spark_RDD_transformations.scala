package Spark_RDD_Transformation

object Spark_RDD_Transformation {
 import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
 
    sc.setLogLevel("ERROR")
    
    val raw_data = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//usdata.csv")
    val data_1 = raw_data.flatMap(x=>x.split(","))
    println("flattened by ," + data_1.count())
    val data_2 = raw_data.filter(x=>x.length()>=180)
                         .flatMap(x=>x.split(","))
    println("Filtered by length >=180 " + data_2.count())
    val data_3 = raw_data.filter(x=>x.length()<180)
                         .flatMap(x=>x.split(","))
    println("Filtered by length <180 " + data_3.count())
  }
  
}