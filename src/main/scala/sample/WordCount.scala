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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.HashPartitioner
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 *
 */
object WordCount extends Base {

  /**
   *
   */
  def main(args: Array[String]) {
    System.setProperty(TezConstants.GENERATE_JAR, "true")
    System.setProperty(TezConstants.UPDATE_CLASSPATH, "true")
    println("######## STARING WordCount")
    var inputFile = "/partitioning.txt"
    var reducers = 2
    if (args != null && args.length > 1) {
      inputFile = args(0)
      reducers = Integer.parseInt(args(1))
    }
    val sparkConf = this.buildSparkConf(this.getClass.getSimpleName())
    val sc = new SparkContext(sparkConf)

    val source = sc.textFile(inputFile)
    val result = source
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_+_, reducers)
      .saveAsNewAPIHadoopFile("out", classOf[Text],
        classOf[IntWritable], classOf[TextOutputFormat[_, _]])
        
    println("######## FINISHED WordCount. Output is in " + 
        FileSystem.get(sc.hadoopConfiguration).makeQualified(new Path("out")))
    sc.stop
  }
}