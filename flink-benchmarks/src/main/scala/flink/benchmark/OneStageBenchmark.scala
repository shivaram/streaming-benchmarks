/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.benchmark

import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.NumberSequenceIterator
import org.apache.flink.api.common.ExecutionConfig

import java.util._
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._

/**
 * This example shows how to use:
 *
 *  - Bulk iterations
 *  - Broadcast variables in bulk iterations
 */
object OneStageBenchmark {

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // env.getConfig.enableObjectReuse()

    val numIterations = params.getInt("iterations", 10)
		val numElemsPerPartition = params.getInt("numElemsPerPart", 10000)
    val numTrials = params.getInt("numTrials", 5)
    val numReducers = params.getInt("numReducers", 16)
		val parallelism = env.getParallelism
		val numElems = numElemsPerPartition.toLong * parallelism

    (0 until numTrials).foreach { t =>
      // val data = env.fromParallelCollection(new NumberSequenceIterator(0L, numElems))
      val data = env.generateSequence(0L, numElems)

      // val result = data.iterate(numIterations) { currentData =>
      //   val res = currentData.map { i => i + 1 }.reduce(_ + _)
      //   // val res1 = res.map { x => println("GOT " + x); x }
      //   currentData.map { i => i + 1 }
      // }
      val bucketSums = data.map { x =>
        (x % numReducers, x)
      }.groupBy(0).sum(1)

      val startTimeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(
          Calendar.getInstance().getTime());
      println(startTimeStamp + ": Using " + numElems + " elems")
      val begin = System.nanoTime()
      // val sum = result.count()
      val localRes = bucketSums.collect()
      val end = System.nanoTime()
      // println("Out is " + localRes.mkString(","))
      val timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(
        Calendar.getInstance().getTime());

      println(timeStamp + ":Running " + numIterations + " took " + (end-begin)/1e6 + " ms")
    }

  }

}
