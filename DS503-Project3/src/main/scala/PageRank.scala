import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank Application").setMaster("local")
    val sc = new SparkContext(conf)

    var links = sc.textFile("input/soc-LiveJournal1.txt").map(line => (line.split("\t")(0), line.split("\t")(1)))
    val header = links.first()
    links = links.filter(row=>row!=header)
    val linkGroup = links.groupByKey().map(pair=>(pair._1,pair._2.toArray))

    var ranks = linkGroup.distinct().map(pair=>(pair._1,1.0))

    for (_ <- 1 to 2) {
      val contrib = linkGroup.join(ranks,2)
      val flatMapRDD = contrib.flatMap{
        case (url,(linkGroup,rank)) => linkGroup.map(dest=>(dest,rank/linkGroup.length))
      }
      ranks = flatMapRDD.reduceByKey(_ + _,2)
    }
    sc.parallelize(ranks.sortBy(_._2,ascending=false).take(100)).saveAsTextFile("output/PageRankN")
  }
}
