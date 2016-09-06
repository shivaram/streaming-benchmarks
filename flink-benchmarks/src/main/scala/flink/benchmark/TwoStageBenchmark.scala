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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to use:
 *
 *  - Bulk iterations
 *  - Broadcast variables in bulk iterations
 */
object TwoStageBenchmark {

  val LOG = LoggerFactory.getLogger("flink.benchmark.TwoStageBenchmark")

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // env.getConfig.enableObjectReuse()

    val numIterations = params.getInt("iterations", 10)
    val numTrials = params.getInt("numTrials", 5)
		val parallelism = env.getParallelism
		val numElems = parallelism
    val startTimeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(
        Calendar.getInstance().getTime());
    println(startTimeStamp + ": Using " + numElems + " elems")

    (0 until numTrials).foreach { t =>
      val data = env.fromParallelCollection(new NumberSequenceIterator(0L, numElems-1)).map(x => x.longValue())

      val begin = System.nanoTime()
      val result = data.iterate(numIterations) { currentData =>
        val newData = currentData.flatMap { x =>
          (0 until parallelism).map { red =>
            (red, x)
          }
        }.groupBy(0).reduceGroup { x =>
          if (!x.hasNext) {
            0L
          } else {
            x.next._1
          }
        }
        newData
      }

      val sum = result.collect()
      val end = System.nanoTime()

      val dataSum = data.collect().sum
      // println("DataMod is " + dataMod.collect().mkString("\n"))
      // println("SUMS ARE " + sum.mkString("\n"))
      println("SUM of SUMS is " + sum.sum)
      println("SUM of data is " + dataSum)
      val timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(
        Calendar.getInstance().getTime());

      println(timeStamp + ":Running " + numIterations + " took " + (end-begin)/1e6 + " ms")
    }
  }

  class SumAddUpdate extends RichMapFunction[(Long, Long), (Long, Long)] {
    private var parameter: Traversable[(Long, Long)] = null

    /** Reads the parameters from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      parameter = getRuntimeContext.getBroadcastVariable[(Long, Long)]("sums").asScala
    }

    def map(in: (Long, Long)): (Long, Long) = {
      var outVal = 0L
      for (p <- parameter) {
        if (p._1 == in._1) {
          outVal = in._2 + p._2
        }
      }
      (in._1, outVal)
    }

  }

	class SubUpdate(numReducers: Int) extends RichMapFunction[Long, (Long, Long)] {

    private var parameter: Traversable[Long] = null

    /** Reads the parameters from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      parameter = getRuntimeContext.getBroadcastVariable[Long]("sums").asScala
    }

    def map(in: Long): (Long, Long) = {
      val inc = in + 10
      val mod = (inc) % numReducers
      (mod, inc)
    }
  }
	
}
