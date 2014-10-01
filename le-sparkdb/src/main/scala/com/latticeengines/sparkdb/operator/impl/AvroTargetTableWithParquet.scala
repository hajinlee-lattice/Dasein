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
import parquet.hadoop.metadata.CompressionCodecName

class AvroTargetTableWithParquet(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[GenericRecord]): RDD[GenericRecord] = {
    val schema = rdd.first().getSchema()
    val path = new Path(getPropertyValue(AvroTargetTableWithParquet.DataPath))
    val job = dataFlow.job
    
    AvroParquetOutputFormat.setSchema(job, schema)
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
    // set a large block size to ensure a single row group.  see discussion
    ParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
    
    job.setOutputFormatClass(classOf[AvroParquetOutputFormat])
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[GenericRecord])
    job.getConfiguration.set("mapred.output.dir", path.toString())
    
    rdd.map(p => (null, p)).saveAsNewAPIHadoopDataset(job.getConfiguration())
    null
  }
}

object AvroTargetTableWithParquet {
  val DataPath = "DataPath"
  val UniqueKeyCol = "UniqueKeyCol"
}