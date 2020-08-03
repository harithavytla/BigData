package Spark_RDD_Transformation

object Spark_RDD_Transformation {
 import org.apache.spark._
 import org.apache.spark.sql.SparkSession
 
 case class txn_schema(txnno : Int, txndate : String,custno : String,amount  : String,category: String,product :String,city :String,state: String,spendby: String)
    
 
  def main(args:Array[String]):Unit={
   
   
    val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    sc.setLogLevel("ERROR")
    
    val txns = sc.textFile("C:///Users//bnama//Desktop//BigData_local//Data//txns")
    val txns_filter=txns.map(x=>x.split(","))
                        .map(x=>txn_schema(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
                        .filter(x=>(x.txnno>5000) & (x.category=="Gymnastics"))
                        .toDF()
    txns_filter.show(5)
    
    
    //txns_filter.map(x=>x(0)+","+x(1)).map(x(0)>5000)foreach(println)
 }
  
}