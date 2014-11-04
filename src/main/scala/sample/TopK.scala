package sample

import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TopK extends Base {
  import org.apache.spark.SparkContext._
  /**
   * args(0): input file location
   * args(1): top - first column
   * args(2): storage level, for testing Spark without TezJobExecutionContext
   */
  def main(args: Array[String]) {

    var input = "/topk_data.txt"
    var top = 5
    var storageLevel = "none"
    if (args != null && args.length > 1) {
      input = args(0)
      top = args(1).toInt
      storageLevel = args.length match {
        case 3 => args(2)
        case _ => "none"
      }
    }

    val sparkConf = this.buildSparkConf(this.getClass.getSimpleName())
    val sc = new SparkContext(sparkConf)

    val txtFile = doPersist(
      sc.textFile(input).map(line => line.split(" ")), storageLevel)


    val result = txtFile
      .map(arr => (arr(0), 1))
      .reduceByKey(_ + _)
      .map(pair => pair.swap)
      .top(top)

    result.foreach{case (number, value) => println (value + " : " + number)}

  }

  def doPersist(rdd: RDD[Array[String]], level: String): RDD[Array[String]] = {
    level match {
      case "cache" => rdd.persist(StorageLevel.MEMORY_ONLY)
      case "mem_and_disk" => rdd.persist(StorageLevel.MEMORY_AND_DISK)
      case "mem_ser" => rdd.persist(StorageLevel.MEMORY_ONLY_SER)
      case "mem_and_disk_ser" => rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      case _ => rdd
    }
  }


}
