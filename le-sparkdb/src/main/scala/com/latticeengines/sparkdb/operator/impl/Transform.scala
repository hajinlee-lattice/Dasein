package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.common.exposed.util.AvroUtils
import com.latticeengines.domain.exposed.eai.AttributeOwner
import com.latticeengines.eai.exposed.util.AvroSchemaBuilder
import com.latticeengines.sparkdb.conversion.Implicits.objectToExpressionList
import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class Transform(val df: DataFlow) extends DataOperator(df) {
  
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    
    val expressionList = getPropertyValue(Transform.ExpressionList)
    
    val expressionGroup = new AttributeOwner()
    for (expression <- expressionList) {
      if (expression.isNew()) {
        expressionGroup.addAttribute(expression.getAttributeToCreate())
      }
    }
    expressionGroup.setSchema(AvroSchemaBuilder.createSchema("t1", expressionGroup))
    val schemaAndMap = AvroUtils.combineSchemas(rdd.first().getSchema(), expressionGroup.getSchema())
    schemaAndMap(0) = schemaAndMap(0).toString()

    val transformed = rdd.map(p => {
      
      for (expression <- expressionList) {
        if (!expression.isNew()) {
          val name = expression.getAttributeToActOn().getName()
          p.put(name, expression.getFunction()(p.get(name)))
        } else {
          
        }
      }
      p
    })
    
    transformed
  }

  override def getPropertyNames(): Set[String] = {
    return Set(Transform.ExpressionList)
  }
}

object Transform {
  val ExpressionList = "ExpressionList"
    
  def combineAndTransformRecords(u: GenericRecord, v: GenericRecord, s: Array[java.lang.Object]): GenericRecord = {
    val combinedRecord = AvroUtils.combineAvroRecords(u, v, s)
    println(combinedRecord)
    combinedRecord
  }
}
