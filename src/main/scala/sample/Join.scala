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

/**
 *
 */
object Join extends Base {

  /**
   *
   */
  def main(args: Array[String]) {
    println("######## STARING JOIN")
    var inputFile1 = "join1.txt"
    var inputFile2 = "join2.txt"
    if (args != null && args.length > 1) {
      inputFile1 = args(0)
      inputFile2 = args(1)
    }
    val sparkConf = this.buildSparkConf(this.getClass.getSimpleName())
    val sc = new SparkContext(sparkConf)

    val source1 = sc.textFile(inputFile1)
    val source2 = sc.textFile(inputFile2)

    val result2 = source2.map { x =>
      val s = x.split(" ")
      (Integer.parseInt(s(0)), s(1))
    }
    
    val result = source1.map { x =>
      val s = x.split(" ")
      val t = (Integer.parseInt(s(2)), (s(0), s(1)))
      t
    }.join(result2).reduceByKey { (x, y) =>
      ((x._1.toString, y._1.toString), x._2)
    }.collect
    
    println("RESULT: " + result)
    
    println("######## FINISHED JOIN")
    sc.stop
  }
}