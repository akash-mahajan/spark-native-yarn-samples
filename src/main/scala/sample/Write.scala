package sample

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Write extends Base {
  /**
   *
   */
  def main(args: Array[String]) {
    println("######## STARING Write Job with Wordcount")
    var inputFile = "/sample-data/wordcount.txt"
    var saveMode = 0
    if (args != null && args.length > 1) {
      inputFile = args(0)
      saveMode = args(1).toInt
    }
    val sparkConf = this.buildSparkConf(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val source = sc.textFile(inputFile)
    val result = source
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_+_)

    saveMode match {
      case 1 =>
        result.saveAsNewAPIHadoopFile("/newApi-out2", classOf[Text],
        classOf[IntWritable], classOf[TextOutputFormat[_, _]])
      case 2 =>
        result.saveAsTextFile("out1")
      case 3 =>
        result.saveAsTextFile("/out2")
      case _ =>
        result.saveAsNewAPIHadoopFile("newApi-out1", classOf[Text],
        classOf[IntWritable], classOf[TextOutputFormat[_, _]])
    }

    println("######## FINISHED Write")
    sc.stop
  }
}
