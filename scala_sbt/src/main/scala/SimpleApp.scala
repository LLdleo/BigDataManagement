/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val inputFile = "input.txt" // 应该是你系统上的某些文件
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val inputData = sc.textFile(inputFile, 2).cache()
    val numAs = inputData.filter(line => line.contains("a")).count()
    val numBs = inputData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
 }