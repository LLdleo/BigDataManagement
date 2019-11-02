/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object SimpleApp {
  def main(args: Array[String]): Unit ={
    val inputFile = "E:\\javaProject\\DS503\\Project3_sbt\\input.txt" // 应该是你系统上的某些文件
    val spark = SparkSession.builder.appName("Simple Application").master("local[2]").getOrCreate()
    val inputData = spark.read.textFile(inputFile).cache()
    val numAs = inputData.filter(line => line.contains("a")).count()
    val numBs = inputData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
//    val inputFile = "input.txt" // 应该是你系统上的某些文件
//    val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")
//    val sc = new SparkContext(conf)
//    val inputData = sc.textFile(inputFile).cache()
//    val numAs = inputData.filter(line => line.contains("a")).count()
//    val numBs = inputData.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    sc.stop()
////Create a SparkContext to initialize Spark
//    val conf = new SparkConf()
//    conf.setMaster("local")
//    conf.setAppName("Word Count")
//    val sc = new SparkContext(conf)
//
//    // Load the text into a Spark RDD, which is a distributed representation of each line of text
//    val textFile = sc.textFile("input.txt")
//
//    //word count
//    val counts = textFile.flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//
//    counts.foreach(println)
//    System.out.println("Total words: " + counts.count());
//    counts.saveAsTextFile("/tmp/shakespeareWordCount")
  }
}
