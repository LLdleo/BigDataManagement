import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext, SparkSession}


object Problem2 {
  class CellBoundary(val x1: Int, val x2: Int, val y1: Int, val y2: Int) extends Serializable {
    override def toString: String = s"($x1, $x2, $y1, $y2)"
  }

  def get_cell_boundary(cell_number: Int, row_length: Int = 500): CellBoundary = {
    val x = (cell_number - 1) % row_length
    val y = Math.floor((cell_number - 1) / row_length).toInt

    val x1 = x * 20
    val x2 = (x + 1) * 20

    val y1 = y * 20
    val y2 = (y + 1) * 20
    new CellBoundary(x1, x2, y1, y2)
  }

  def points_in_cell(point: Array[String], cell: CellBoundary): Boolean = {
    val x = point(0).toInt
    val y = point(1).toInt
    (x >= cell.x1) && (x < cell.x2) && (y >= cell.y1) && (y < cell.y2)
  }

  def get_cell_number_for_point(point: Array[String], row_length: Int = 500, cell_size: Int = 20): Int = {
    val x = point(0).toInt
    val y = point(1).toInt
    (row_length - 1 - y / cell_size) * row_length + x / cell_size + 1
  }

  def get_cell_neighbors(cell_number: Int, row_length: Int = 500): Array[Int] = {
    var neighbor_cells = Array[Int]()
    var has_left = true
    var has_right = true
    var has_top = true
    var has_bottom = true
    if (cell_number % row_length != 1) {neighbor_cells ++= Array[Int](cell_number-1)} else {has_left = false}
    if (cell_number % row_length != 0) {neighbor_cells ++= Array[Int](cell_number+1)} else {has_right = false}
    if (cell_number > row_length) {neighbor_cells ++= Array[Int](cell_number-row_length)} else {has_top = false}
    if (cell_number <= (row_length-1)*row_length) {neighbor_cells ++= Array[Int](cell_number+row_length)} else {has_bottom = false}
    if (has_left & has_top) {neighbor_cells ++= Array[Int](cell_number-row_length-1)}
    if (has_right & has_top) {neighbor_cells ++= Array[Int](cell_number-row_length+1)}
    if (has_left & has_bottom) {neighbor_cells ++= Array[Int](cell_number+row_length-1)}
    if (has_right & has_bottom) {neighbor_cells ++= Array[Int](cell_number+row_length+1)}
    neighbor_cells
  }

  def get_value(cell_number:Int, CellCountMap:Map[Int,Int]): Int = {
    try {
      CellCountMap(cell_number)
    } catch {
      case e: Exception => 0
    }

  }

  def calculate(cell_number:Int, CellCountMap:Map[Int, Int], row_length: Int = 500): Float ={
    val x_count = get_value(cell_number, CellCountMap)

    val neighbors = get_cell_neighbors(cell_number, row_length)
    val y_count = neighbors.map(number => get_value(number, CellCountMap))
    val y_count_average = y_count.sum.toFloat / y_count.length.toFloat
    x_count.toFloat / y_count_average.toFloat
  }

  def calculate_neighbors_index(cell_number:Int, CellCountMap:Map[Int, Int], row_length:Int=500):Array[Float]={
    val neighbors = get_cell_neighbors(cell_number,row_length)
    neighbors.map(v => calculate(v, CellCountMap, row_length))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Problem2 Application").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Problem2 Application SQL").master("local").getOrCreate()
    import spark.implicits._
    val length = 500

    val pointsRDD = sc.textFile("input/Points").map(line => line.split(",").map(ele => ele.trim))
//    val pointsDict = pointsRDD.groupBy(point => get_cell_number_for_point(point))
//    pointsDict.saveAsTextFile("output/group")
//    val cellCount = pointsDict.mapValues(sq => sq.sum)
    val pointsCell = pointsRDD.map(point => get_cell_number_for_point(point)).toDF("CellNum")
    pointsCell.createOrReplaceTempView("Cells")
    val cellCount = spark.sql("SELECT CellNum, COUNT(1) as CellCount FROM Cells GROUP BY CellNum")  //every row: 1234, 12
//    println(cellCount.filter("cellNum=216847").select("CellCount").collect().head)

    val cellRDD = cellCount.rdd.map(row => (row(0).toString.toInt,row(1).toString.toInt))
    val cellCountMap = cellRDD.map(pair=>Map[Int,Int](pair._1 -> pair._2)).collect().flatten.toMap

    val densityRDD = cellRDD.map(pair => (pair._1,calculate(pair._1,cellCountMap,length)))
    val top50_i = densityRDD.sortBy(_._2,ascending=false).take(50)
    val top50_i_rdd = sc.parallelize(top50_i)
    // this is the result of the Top 50 grid cells w.r.t Relative-DensityIndex
    top50_i_rdd.saveAsTextFile("output/top50_index")  // save top 50 index into output/top50_i

    // this is the result of the Neighbors of the Top 50
    val new_rdd = top50_i_rdd.map(pair=>(pair._1,calculate_neighbors_index(pair._1,cellCountMap,length).toList))
    new_rdd.saveAsTextFile("output/top50_neighbors_index")

  }
}
