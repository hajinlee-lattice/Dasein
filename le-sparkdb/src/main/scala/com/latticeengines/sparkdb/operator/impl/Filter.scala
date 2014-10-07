package com.latticeengines.sparkdb.operator.impl

import scala.Array.canBuildFrom
import scala.collection.JavaConversions._

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.domain.exposed.sparkdb.FunctionExpression
import com.latticeengines.sparkdb.conversion.Implicits.objectToExpression
import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class Filter(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val filterCondition = getPropertyValue(Filter.FilterCondition)
    rdd.filter(record => Filter.filterFunction(record, filterCondition))
  }

  override def getPropertyNames(): Set[String] = {
    return Set(Filter.FilterCondition)
  }

}

object Filter {
  val FilterCondition = "FilterCondition"
    
  def filterFunction(record: GenericRecord, condition: FunctionExpression): Boolean = {
    val values = condition.getSourceAttributes().map(x => record.get(x.getName()))
    condition.getFunction().apply(values: _*).asInstanceOf[Boolean]
  }
}