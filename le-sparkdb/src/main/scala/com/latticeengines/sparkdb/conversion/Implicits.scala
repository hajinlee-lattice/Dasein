package com.latticeengines.sparkdb.conversion

import org.apache.spark.rdd._
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

object Implicits {

  implicit def stringToBoolean(value: String): Boolean = value.toBoolean

  implicit def stringToDouble(value: String): Double = java.lang.Double.parseDouble(value)
  
  implicit def recordRDDToGenericRecordRDD(rdd: RDD[Record]): RDD[GenericRecord] = rdd.asInstanceOf[RDD[GenericRecord]]
}