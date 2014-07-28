package com.latticeengines.sparkdb.operator.impl

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator.DataOperator

class DataProfileOperator extends DataOperator {
  override def run(rdd: RDD[GenericRecord], job: Job, sc: SparkContext): RDD[GenericRecord] = {
    val fields = getFields(rdd)
    
    for (f <- fields) {
      val name = f.name()
      val sum = rdd.map(
        p => {
            val value = p.get(name) 
            if (value.isInstanceOf[Float]) {
              value.asInstanceOf[Float]
            } else if (value.isInstanceOf[Int]) {
              value.asInstanceOf[Int]
            } else {
              0.0
            }
        }).reduce(_ + _)

      val avg = sum/rdd.count()
      print(s"Avg for $name = $avg\n")
    }
    null
  }
}

object DataProfileOperator extends App {
  
  override def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("AvroTest")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val job = new Job(conf)
    val path = new Path("/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/samples/allTraining-r-00000.avro")
    
    val source = new AvroSourceTable()
    source.setPropertyValue(AvroSourceTable.DataPath, "/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/samples/allTraining-r-00000.avro")
    
    val profiler = new DataProfileOperator()
    profiler.run(source.run(null, job, sc), job, sc)
    sc.stop()
  }
}
