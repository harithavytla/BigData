package Spark_Gym

object Spark_Gym {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("hdfs:/hive/data/txns/part-m-00000")
    val gym_data = data.filter(x=>x.contains("Gymnastics"))
    gym_data.saveAsTextFile("hdfs:/hive/data/txns/scala")
    
  }
  
}