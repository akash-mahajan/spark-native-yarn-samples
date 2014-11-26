package sample

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.Statistics

object Correlation extends Base {

  def main(args: Array[String]) = {
    println("######## STARING CORRELATION")

    var inputFile = "/correlation_data.txt"
    var featureIndices = Array(0,2,3)
    if (args != null && args.length > 1) {
      inputFile = args(0)
      featureIndices = args(1).split(",").map(_.toInt)
    }
    val sparkConf = this.buildSparkConf(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val vectors = sc.textFile(inputFile)
      .mapPartitions(_.drop(1)) // drop header
      .map(line => Vectors.dense(filterByFeatureIndices(line.split(","), featureIndices).map(_.toDouble)))

    vectors.saveAsTextFile("/user/root/out")
    val corr: Matrix = Statistics.corr(vectors, "pearson")

    // count upper triangular matrix without diagonal
    val num = corr.numRows
    val result = for ((x, i) <- corr.toArray.zipWithIndex if (i / num) < i % num )
    yield (i / num, i % num, new java.text.DecimalFormat("#.######").format(x).toDouble)

    println("RESULT: (<column1>, <column2>, <correlation>)")
    println(result.mkString(","))
    println("######## FINISHED CORRELATION")
    sc.stop
  }

  def filterByFeatureIndices(columns: Array[String], features: Array[Int]): Array[String] = {
    if (features.size == 1 && features(1) == -1){
      columns
    } else {
      {for ((x, index) <- columns.view.zipWithIndex if features contains index) yield x}.toArray
    }
  }

}
