import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object step2 {
  class CellCoordinates(val x1: Int, val x2: Int, val y1: Int, val y2: Int) extends Serializable {
    override def toString: String = s"($x1, $x2, $y1, $y2)"
  }

  def get_cell_coordinates(cell_number: Int, row_length: Int = 500): CellCoordinates = {
    val x = (cell_number - 1) % row_length
    val y = Math.floor((cell_number - 1) / row_length).toInt

    val x1 = x * 20
    val x2 = (x + 1) * 20

    val y1 = y * 20
    val y2 = (y + 1) * 20
    new CellCoordinates(x1, x2, y1, y2)
  }

  def is_point_in_cell(point: Array[String], cell: CellCoordinates): Boolean = {
    val x = point(0).toInt
    val y = point(1).toInt
    (x >= cell.x1) && (x < cell.x2) && (y >= cell.y1) && (y < cell.y2)
  }

  def get_cell_number_for_point(point: Array[String], row_length: Int = 500, cell_size: Int = 20): Int = {
    val x = point(0).toInt
    val y = point(1).toInt
    (499 - y / cell_size) * row_length + x / cell_size + 1
  }

  def get_cell_neighbors(cell_number: Int, row_length: Int = 500): Array[Int] = {
    var not_in_list = Array()
    for (i <- 0 until row_length) {
      if (i == 0 or i == row_length-1) {
        for (j <- 1 to row_length) {
          not_in_list +=  i*row_length+j
        }
      }
      else {
        var not_in_list = not_in_list + Array[int](i*row_length+1,(i+1)*row_length)
      }
    }
    val top_neighbor_number = cell_number - row_length
    val bottom_neighbor_number = cell_number + row_length
    var neighbor_numbers = Array[Int](top_neighbor_number, bottom_neighbor_number)

    if ((cell_number - 1) % 500 != 0) {
      val left_neighbor_number = cell_number - 1
      val top_left_neighbor_number = top_neighbor_number - 1
      val bottom_left_neighbor_number = bottom_neighbor_number - 1
      neighbor_numbers = neighbor_numbers ++ Array[Int](left_neighbor_number,
        top_left_neighbor_number,
        bottom_left_neighbor_number)
    }

    if (cell_number % 500 != 0) {
      val right_neighbor_number = cell_number + 1
      val top_right_neighbor_number = top_neighbor_number + 1
      val bottom_right_neighbor_number = bottom_neighbor_number + 1
      neighbor_numbers = neighbor_numbers ++ Array[Int](right_neighbor_number,
        top_right_neighbor_number,
        bottom_right_neighbor_number)
    }

    neighbor_numbers.filter(number => number > 0)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Step2 Application").setMaster("local")
    val sc = new SparkContext(conf)

    val pointsRDD = sc.textFile("Points").map(line => line.split(",").map(elem => elem.trim))

    //  Cells with count of points
    val cellPointsRDD = pointsRDD.map({point => (get_cell_number_for_point(point), 1)}).
      reduceByKey((x, y) => x + y)

    // file line count
    val lines = pointsRDD.count()
    println(lines)
    if (false) {
      // Density for cell
      val cell_number = 502
      val cell = get_cell_coordinates(cell_number)
      val cell501RDD = pointsRDD.filter(point => is_point_in_cell(point, cell))
      val x_count = cell501RDD.count

      val cell501NeighborNumbers = get_cell_neighbors(cell_number)
      val y_count = cell501NeighborNumbers.map(number => get_cell_coordinates(number)).
        map(neighbor => pointsRDD.filter(point => is_point_in_cell(point, neighbor))).
        map(rdd => rdd.count)
      val y_count_average = y_count.sum / y_count.length

      println(s"$x_count / $y_count_average = ${x_count.toFloat / y_count_average.toFloat}")
    }

  }
}
