package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.metadata.InterfaceName

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import util.control.Breaks.break
import util.control.Breaks.breakable

class MergeInGroup(schema: StructType, overwriteByNull: Boolean) extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = schema
    override def bufferSchema: StructType = schema
    override def dataType: DataType = schema
    override def deterministic: Boolean = true
    
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        for (idx <- 0 to schema.length - 1) {
            buffer(idx) = null
        }
    }
    
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        for (idx <- 0 to schema.length - 1) {
            breakable {
                if (InterfaceName.isEntityId(schema.fieldNames(idx))) {
                    if (overwriteEntityId(buffer.getAs[String](idx), input.getAs[String](idx))) {
                        buffer(idx) = input.getAs[String](idx)
                    }
                    break
                }
                if (overwriteByNull || input.getAs[Any](idx) != null) {
                    buffer(idx) = input.getAs[Any](idx)
                }                
            }
        }
    }
    
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        for (idx <- 0 to schema.length - 1) {
            breakable {
                if (InterfaceName.isEntityId(schema.fieldNames(idx))) {
                    if (overwriteEntityId(buffer1.getAs[String](idx), buffer2.getAs[String](idx))) {
                        buffer1(idx) = buffer2.getAs[String](idx)
                    }
                    break
                }
                if (overwriteByNull || buffer2.getAs[Any](idx) != null) {
                    buffer1(idx) = buffer2.getAs[Any](idx)
                }                
            }
        }
    }
    
    override def evaluate(buffer: Row): Any = {
        buffer
    }
    
    private def overwriteEntityId(currId: String, newId: String): Boolean = {
        if (newId == null) {
            return false
        }
        if (currId != null && currId != DataCloudConstants.ENTITY_ANONYMOUS_ID && newId == DataCloudConstants.ENTITY_ANONYMOUS_ID) {
            return false
        }
        true
    }
}