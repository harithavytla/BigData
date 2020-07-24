package Spark_Gym

object Spark_Gym {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
 
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//usdata.csv")
                 .filter(x=>x.length()>200)
                 .flatMap(x=>x.split(","))
                 .flatMap(x=>x.split(" "))
                 .filter(x=>x.contains("\""))
                 .foreach(println)
    
  }
  
}