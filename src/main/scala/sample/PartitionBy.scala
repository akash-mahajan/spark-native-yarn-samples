/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sample

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.tez.TezJobExecutionContext
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.tez.TezConstants
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.HashPartitioner

/**
 *
 */
object PartitionBy extends Base {

  /**
   *
   */
  def main(args: Array[String]) {
    println("######## STARING PARTITION BY")
    var inputFile = "/partitioning.txt"
    if (args != null && args.length > 0) {
      inputFile = args(0)
    }
    val sparkConf = this.buildSparkConf(this.getClass.getSimpleName())
    val sc = new SparkContext(sparkConf)

    val source = sc.textFile(inputFile)
    val result = source
      .map { s => val split = s.split("\\s+", 2); (split(0).replace(":", "_"), split(1)) }
      .partitionBy(new HashPartitioner(2))
      .saveAsHadoopFile(sc.appName + "_out", classOf[Text], classOf[Text], classOf[KeyPerPartitionOutputFormat])
    println("######## FINISHED PARTITION BY")
    sc.stop
  }
}

/**
 *
 */
class KeyPerPartitionOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key.toString
  }
}