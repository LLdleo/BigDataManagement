import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, DataFrameReader}

import scala.collection._


object step2 {
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

  def calculate(cell_number:Int, pointsRDD:RDD[Array[String]]): Float ={
    val cell = get_cell_boundary(cell_number)
    val cell501RDD = pointsRDD.filter(point => points_in_cell(point, cell))
    val x_count = cell501RDD.count

    val neighbors = get_cell_neighbors(cell_number)
    val y_count = neighbors.map(number => get_cell_boundary(number)).
      map(neighbor => pointsRDD.filter(point => points_in_cell(point, neighbor))).
      map(rdd => rdd.count)
    val y_count_average = y_count.sum / y_count.length
    x_count.toFloat / y_count_average.toFloat
  }

  case class cell_number_density(cell_number: Int, density: Float)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Step2 Application").setMaster("local")
    val sc = new SparkContext(conf)

    val pointsRDD = sc.textFile("Points").map(line => line.split(",").map(elem => elem.trim))

    //  Cells with count of points
    val cellPointsRDD = pointsRDD.map({point => (get_cell_number_for_point(point), 1)}).reduceByKey((x, y) => x + y)

    var density_list = mutable.MutableList[cell_number_density]()
    for (cell_number <- 1 to 250000) {
      density_list += cell_number_density(cell_number, calculate(cell_number,pointsRDD))
    }
    // val densityDF = density_list.toDF()
    val cell_number = 502
    println(calculate(cell_number,pointsRDD))
  }
}
