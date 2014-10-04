package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.conversion.Implicits.objectToBoolean
import com.latticeengines.sparkdb.conversion.Implicits.objectToDouble
import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator


class Sampler(val df: DataFlow) extends DataOperator(df) {

  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val withReplacement = getPropertyValue(Sampler.WithReplacement)
    val samplingRate = getPropertyValue(Sampler.SamplingRate)
    rdd.sample(withReplacement, samplingRate)
  }

  override def getPropertyNames(): Set[String] = {
    return Set(Sampler.WithReplacement, Sampler.SamplingRate)
  }
}

object Sampler {
  val WithReplacement = "WithReplacement"
  val SamplingRate = "SamplingRate"
  
}