package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.DataOperator

class AvroTargetTable(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val schema = rdd.first().getSchema()
    val path = new Path(getPropertyValue(AvroTargetTable.DataPath))
    val job = dataFlow.job
    job.setOutputKeyClass(classOf[AvroKey[GenericRecord]])
    job.setOutputValueClass(classOf[NullWritable])
    job.setOutputFormatClass(classOf[AvroKeyOutputFormat[AvroKey[GenericRecord]]])
    job.getConfiguration.set("mapred.output.dir", path.toString())
    AvroJob.setOutputKeySchema(job, schema)
    rdd.map(p => (new AvroKey[GenericRecord](p), NullWritable.get())).saveAsNewAPIHadoopDataset(job.getConfiguration())
    null
  }
}

object AvroTargetTable {
  val DataPath = "DataPath"
  val UniqueKeyCol = "UniqueKeyCol"
}