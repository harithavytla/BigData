package Schema_RDD

object Schema_RDD {
  import org.apache.spark._
  
  
  def main(args:Array[String]) : Unit={
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//txns")
    val data_filter = data.map(x=> if(x.contains("Gymnastics")) x+", YES" else(x+", NO"))
    data_filter.take(10).foreach(println)
                     
    
  }
}