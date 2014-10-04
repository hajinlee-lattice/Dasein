package com.latticeengines.sparkdb.operator.impl

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class DataProfileOperator(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val fields = getFields(rdd.first())
    
    for (f <- fields) {
      val name = f.name()
      val sum = rdd.map(
        p => {
            val value = p.get(name) 
            if (value.isInstanceOf[Float]) {
              value.asInstanceOf[Float]
            } else if (value.isInstanceOf[Int]) {
              value.asInstanceOf[Int]
            } else if (value.isInstanceOf[Double]) {
              value.asInstanceOf[Double]
            } else {
              0.0
            }
        }).reduce(_ + _)

      val avg = sum/rdd.count()
      print(s"Avg for $name = $avg\n")
    }
    null
  }

  override def getPropertyNames(): Set[String] = {
    return Set()
  }
}