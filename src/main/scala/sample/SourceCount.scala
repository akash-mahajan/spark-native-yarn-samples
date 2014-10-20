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

/**
 * 
 */
object SourceCount extends Base {

  /**
   * 
   */
  def main(args: Array[String]) {
    println("######## STARING SOURCE COUNT")
    // used standard Hadoop randomtextgenerator 
    var inputFile = "/random-text-data"
    if (args != null && args.length > 0) {
      inputFile = args(0)
    } 
    val sparkConf = this.buildSparkConf(this.getClass.getSimpleName())
    val sc = new SparkContext(sparkConf)
    
    val source = sc.textFile(inputFile)
    val result = source.count
    println("Result: " + result)
    println("######## FINISHED SOURCE COUNT")
    sc.stop
  }
}