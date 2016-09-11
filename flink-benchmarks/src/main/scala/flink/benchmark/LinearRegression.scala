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

import breeze.linalg._
import breeze.math._

import scala.collection.JavaConverters._

object LinearRegression {

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val numDataPoints = params.getLong("numDataPoints", 100000L)
    val numDims = params.getInt("numDims", 100)
    val stepSize = params.getDouble("stepSize", 0.01)
    
    val rnd = new java.util.Random(123L)

    val data = env.generateSequence(0L, numDataPoints).map { x =>
      new Data(DenseVector.rand[Double](numDims), rnd.nextDouble())
    }

    val parameters = env.fromCollection(Seq(DenseVector.zeros[Double](numDims)))

    // val data =
    //     env.readCsvFile[(Double, Double)](
    //       params.get("input"),
    //       fieldDelimiter = " ",
    //       includedFields = Array(0, 1))
    //       .map { t => new Data(t._1, t._2) }

    val numIterations = params.getInt("iterations", 10)

    val result = parameters.iterate(numIterations) { currentParameters =>
      val newParameters = data
        .map(new SubUpdate(stepSize, numDataPoints)).withBroadcastSet(currentParameters, "parameters")
        .reduce { (p1, p2) =>
          p1 += p2
          p1
        }
      newParameters
    }

    val out = result.collect()
    println("NORM of ouptut " + norm(out.head))
    println("First 5 of output " + out.head.toArray.take(5).mkString(","))
  }

  /**
   * A simple data sample, x means the input, and y means the target.
   */
  case class Data(var x: DenseVector[Double], var y: Double)

  // *************************************************************************
  //     USER FUNCTIONS
  // *************************************************************************

  /**
   * Compute a single BGD type update for every parameters.
   */
  class SubUpdate(stepSize: Double, numPoints: Long) extends RichMapFunction[Data, DenseVector[Double]] {

    private var parameter: DenseVector[Double] = null

    /** Reads the parameters from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      val parameters = getRuntimeContext.getBroadcastVariable[DenseVector[Double]]("parameters").asScala
      parameter = parameters.head
    }

    def map(in: Data): DenseVector[Double] = {

      val diff: Double = (parameter.t * in.x) - in.y
      val grad = in.x.copy
      grad *= diff
      grad *= stepSize

      val out = parameter / numPoints.toDouble - grad

      // val theta0 =
      //   parameter.theta0 - 0.01 * ((parameter.theta0 + (parameter.theta1 * in.x)) - in.y)
      // val theta1 =
      //   parameter.theta1 - 0.01 * (((parameter.theta0 + (parameter.theta1 * in.x)) - in.y) * in.x)
      out
    }
  }
}
