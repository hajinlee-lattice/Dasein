package com.latticeengines.sparkdb.conversion

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.domain.exposed.sparkdb.FunctionExpression

object Implicits {

  implicit def objectToBoolean(value: Object): Boolean = value.asInstanceOf[Boolean]

  implicit def objectToDouble(value: Object): Double = value.asInstanceOf[Double]
  
  implicit def objectToString(value: Object): String = value.asInstanceOf[String]
  
  implicit def objectToExpressionList(value: Object): List[FunctionExpression] = value.asInstanceOf[List[FunctionExpression]]
  
  implicit def anyRDDToGenericRecordRDD(rdd: RDD[_]): RDD[GenericRecord] = rdd.asInstanceOf[RDD[GenericRecord]]
}