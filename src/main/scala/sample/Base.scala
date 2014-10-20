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

import org.apache.spark.SparkConf
import org.apache.spark.tez.TezJobExecutionContext

class Base {

  /**
   *
   */
  def buildSparkConf(applicationName:String): SparkConf = {
    val sparkConf = new SparkConf
    var appName = applicationName
    if (appName.endsWith("$")){
      appName = appName.substring(0, appName.length()-1)
    }
    sparkConf.setAppName(appName)
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("execution-context:" + classOf[TezJobExecutionContext].getName)
    }
    sparkConf.set("spark.ui.enabled", "false")
    sparkConf
  }
}