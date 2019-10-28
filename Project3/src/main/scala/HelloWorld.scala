package scala
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val logFile = "log/helloSpark.log"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(logFile)
    val counts = rdd.flatMap(line=>line.split(",")).map(x=>(x,1)).reduceByKey((x,y)=>(x+y))
    counts.foreach(println)
    sc.stop()
  }
}