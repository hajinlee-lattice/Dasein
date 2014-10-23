package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericModifiableData.ModifiableRecord
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD

import com.latticeengines.common.exposed.util.AvroUtils
import com.latticeengines.sparkdb.conversion.Implicits.anyRDDToGenericRecordRDD
import com.latticeengines.sparkdb.conversion.Implicits.objectToString
import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class AvroSourceTable(val df: DataFlow) extends DataOperator(df) {
  df.addSourceOperator(this)
  
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val conf = dataFlow.job.getConfiguration()
    val path = new Path(getPropertyValue(AvroSourceTable.DataPath))
    val schema = AvroUtils.getSchema(conf, path)

    AvroJob.setInputKeySchema(dataFlow.job, schema)
    dataFlow.sc.newAPIHadoopFile(
      path.toString(),
      classOf[AvroKeyInputFormat[GenericRecord]],
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable], conf).map(x => { new ModifiableRecord(x._1.datum().asInstanceOf[Record]) }).persist()
  }
  
  override def getPropertyNames(): Set[String] = {
    return Set(AvroSourceTable.DataPath, AvroSourceTable.UniqueKeyCol)
  }
  
}

object AvroSourceTable {
  val DataPath = "DataPath"
  val UniqueKeyCol = "UniqueKeyCol"
}