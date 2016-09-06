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

import org.apache.flink.streaming.api.functions._
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.NumberSequenceIterator
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.time.{Time => CTime}
import org.apache.flink.api.common.restartstrategy.RestartStrategies

import java.util._
import java.util.concurrent.TimeUnit

import java.text.SimpleDateFormat

import scala.collection.JavaConverters._

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to use:
 *
 *  - Bulk iterations
 *  - Broadcast variables in bulk iterations
 */
object StreamMicroBenchmark {

  val LOG = LoggerFactory.getLogger("flink.benchmark.TwoStageBenchmark")

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // env.getConfig.enableObjectReuse()

    val numIters = params.getInt("iterations", 10) 
    val elemsPerIterPerPart = params.getInt("elemsPerIter", 10000)
    val numReducers = params.getInt("numReducers", 16)
    val numTrials = params.getInt("numTrials", 5)
    val outPath = params.get("outPath", "/tmp/out.csv")
    val timeWindow = params.getInt("windowTimeInMs", 100)

    env.setBufferTimeout(params.getLong("flinkBufferTimeout", 100))
    env.enableCheckpointing(params.getLong("flinkCheckpointInterval", 1000))
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			3, // number of restart attempts 
			CTime.of(1, TimeUnit.SECONDS) // delay
		))


		val parallelism = env.getParallelism
		val numElems = elemsPerIterPerPart * numIters * parallelism

    val startTimeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(
        Calendar.getInstance().getTime())
    println(startTimeStamp + ": Using " + numElems + " elems " + timeWindow + " ms")

    val data = env.generateSequence(0L, numElems)

    (1 to numTrials).foreach { x =>
      val result = data.map { x =>
        (x % numReducers, x)
      }.keyBy(0).sum(1).keyBy(0).timeWindow(Time.milliseconds(timeWindow)).max(1)
      //}.keyBy(0).timeWindow(Time.milliseconds(timeWindow)).sum(1)

      val beginMillis = System.currentTimeMillis()
      result.map(x => (x._1, x._2, System.currentTimeMillis - beginMillis)).writeAsCsv(
         outPath + "-" + x, writeMode = FileSystem.WriteMode.OVERWRITE)
      
      val begin = System.nanoTime()
      env.execute()
      val end = System.nanoTime()
      val timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(
        Calendar.getInstance().getTime());

      println(timeStamp + ":Running " + numIters + " took " + (end-begin)/1e6 + " ms")
    }
  }
}
