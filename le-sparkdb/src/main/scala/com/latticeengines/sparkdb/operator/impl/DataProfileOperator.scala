package com.latticeengines.sparkdb.operator.impl

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.latticeengines.common.exposed.util.AvroUtils
import com.latticeengines.sparkdb.operator.DataOperator

class DataProfileOperator extends DataOperator {
  
  private var name: String = ""
  
  override def getName(): String = {
    name
  }

  override def setName(name: String) = {
    this.name = name;
  }
  
  override def run[T](rdd: RDD[T]) = {
    // create multiple RDDs per column
  }
}

object DataProfileOperator {
  
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("AvroTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val job = new Job(conf)
    val path = new Path("/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/samples/allTraining-r-00000.avro");
    val schema = AvroUtils.getSchema(conf, path);

    AvroJob.setInputKeySchema(job, schema);
    
    val rdd = sc.newAPIHadoopFile(
       path.toString(),
       classOf[AvroKeyInputFormat[GenericRecord]],
       classOf[AvroKey[GenericRecord]],
       classOf[NullWritable], conf).map(x => x._1.datum())
    val sum = rdd.map { p => p.get("SEPAL_WIDTH").asInstanceOf[Float] }.reduce(_ + _)
    val avg = sum/rdd.count()
    println(s"Sum = $sum")
    println(s"Avg = $avg")
  }
}
