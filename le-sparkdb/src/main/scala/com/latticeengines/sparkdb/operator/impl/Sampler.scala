package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator


class Sampler(val df: DataFlow) extends DataOperator(df) {

    override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val withReplacement = getPropertyValue(Sampler.WithReplacement).asInstanceOf[Boolean]
    val samplingRate = getPropertyValue(Sampler.SamplingRate).asInstanceOf[Double]
    val sample = rdd.sample(withReplacement, samplingRate).asInstanceOf[RDD[GenericRecord]]
    sample
  }

  override def getPropertyNames(): Set[String] = {
    return Set(Sampler.WithReplacement, Sampler.SamplingRate)
  }
}

object Sampler {
  val WithReplacement = "WithReplacement"
  val SamplingRate = "SamplingRate"
  
}