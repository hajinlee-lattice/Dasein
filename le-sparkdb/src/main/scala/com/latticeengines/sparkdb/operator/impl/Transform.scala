package com.latticeengines.sparkdb.operator.impl

import scala.Array.canBuildFrom
import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import org.apache.avro.Schema
import org.apache.avro.generic.GenericModifiableData
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD

import com.latticeengines.common.exposed.util.AvroUtils
import com.latticeengines.domain.exposed.eai.AttributeOwner
import com.latticeengines.domain.exposed.sparkdb.FunctionExpression
import com.latticeengines.eai.exposed.util.AvroSchemaBuilder
import com.latticeengines.sparkdb.conversion.Implicits.objectToExpressionList
import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class Transform(val df: DataFlow) extends DataOperator(df) {
  
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val expressionList = getPropertyValue(Transform.ExpressionList)
    val expressionGroup = new AttributeOwner()
    val allAttrsWithChangedSchemaGroup = new AttributeOwner()
    val allExpressions = Map[String, FunctionExpression]()
    

    expressionList.foreach(p => {
      val targetAttr = p.getTargetAttribute()
      if (p.isNew()) {
        expressionGroup.addAttribute(targetAttr)
      } else {
        for (sourceAttr <- p.getSourceAttributes()) {
          if (sourceAttr.getName().equals(targetAttr.getName()) &&
              !sourceAttr.getPhysicalDataType().equals(targetAttr.getPhysicalDataType())) {
            allAttrsWithChangedSchemaGroup.addAttribute(targetAttr)
          } 
        }
      }
      
      allExpressions += p.getTargetAttribute().getName() -> p
    })

    val schema = AvroSchemaBuilder.mergeSchemas(rdd.first().getSchema(), allAttrsWithChangedSchemaGroup)
    val expressionSchema = AvroSchemaBuilder.createSchema("t1", expressionGroup)
    val schemaAndMap = AvroUtils.combineSchemas(schema, expressionSchema)
    schemaAndMap(0) = schemaAndMap(0).toString()
    val expressionSchemaStr = expressionSchema.toString()
    
    val transformed = rdd.map(p => {
      val combinedSchema = Schema.parse(schemaAndMap(0).asInstanceOf[String])
      val exprSchema = Schema.parse(expressionSchemaStr)
      val exprRecord = new GenericModifiableData.ModifiableRecord(exprSchema)
      val combinedRecord = AvroUtils.combineAvroRecords(p, exprRecord, schemaAndMap)

      for (f <- combinedSchema.getFields()) {
        val name = f.name()
        val functionExpression = allExpressions.getOrElse(name, null)
        
        if (functionExpression != null) {
            val values = functionExpression.getSourceAttributes().map(x => {
              combinedRecord.get(x.getName())
            })
            combinedRecord.put(functionExpression.getTargetAttribute().getName(),
                functionExpression.getFunction().apply(values: _*))
        }
      }
      combinedRecord
    })
    
    transformed
  }

  override def getPropertyNames(): Set[String] = {
    return Set(Transform.ExpressionList)
  }
}

object Transform {
  val ExpressionList = "ExpressionList"
    
}
