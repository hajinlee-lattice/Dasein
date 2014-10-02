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
import parquet.avro.AvroParquetOutputFormat
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName;

import com.latticeengines.sparkdb.conversion.Implicits._


class AvroTargetTable(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val schema = rdd.first().getSchema()
    val path = new Path(getPropertyValue(AvroTargetTable.DataPath))
    val job = dataFlow.job
    
    if (getPropertyValue(AvroTargetTable.ParquetFile)) {
      AvroParquetOutputFormat.setSchema(job, schema)
      ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
      ParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024) 
    
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[GenericRecord])
      job.setOutputFormatClass(classOf[AvroParquetOutputFormat])
      job.getConfiguration.set("mapred.output.dir", path.toString())
      rdd.map(p => (null, p)).saveAsNewAPIHadoopDataset(job.getConfiguration())
    } else {
      job.setOutputKeyClass(classOf[AvroKey[GenericRecord]])
      job.setOutputValueClass(classOf[NullWritable])
      job.setOutputFormatClass(classOf[AvroKeyOutputFormat[AvroKey[GenericRecord]]])
      job.getConfiguration.set("mapred.output.dir", path.toString())
      AvroJob.setOutputKeySchema(job, schema)
      rdd.map(p => (new AvroKey[GenericRecord](p), NullWritable.get())).saveAsNewAPIHadoopDataset(job.getConfiguration())
    }
    null
  }

  override def getPropertyNames(): Set[String] = {
    return Set(AvroTargetTable.DataPath, AvroTargetTable.UniqueKeyCol, AvroTargetTable.ParquetFile)
  }
}

object AvroTargetTable {
  val DataPath = "DataPath"
  val UniqueKeyCol = "UniqueKeyCol"
  val ParquetFile = "ParquetFile"
}