package com.latticeengines.sparkdb.operator.impl

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.latticeengines.sparkdb.operator._

class DataProfileOperator(val df: DataFlow) extends DataOperator(df) {
  override def run(rdd: RDD[(Int, GenericRecord)]): RDD[(Int, GenericRecord)] = {
    val fields = getFields(rdd.first()._2)
    
    for (f <- fields) {
      val name = f.name()
      val sum = rdd.map(
        p => {
            val value = p._2.get(name) 
            if (value.isInstanceOf[Float]) {
              value.asInstanceOf[Float]
            } else if (value.isInstanceOf[Int]) {
              value.asInstanceOf[Int]
            } else if (value.isInstanceOf[Double]) {
              value.asInstanceOf[Double]
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
    val conf = new Configuration()
    val dataFlow = new DataFlow("AvroTest", conf, true)

    val source1 = new AvroSourceTable(dataFlow)
    source1.setPropertyValue(AvroSourceTable.DataPath, "/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/samples/allTraining-r-00000.avro")
    source1.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Nutanix_EventTable_Clean")
    
    val source2 = new AvroSourceTable(dataFlow)
    source2.setPropertyValue(AvroSourceTable.DataPath, "/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/samples/allTraining-r-00000.avro")
    source2.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Nutanix_EventTable_Clean")

    val filter = new Filter(dataFlow)
    
    val join = new Join(dataFlow)
    
    val profiler = new DataProfileOperator(dataFlow)
    
    //profiler.run(filter.run(source1.run(null)))
    profiler.run(filter.run(join.run(Array(source1.run(null), source2.run(null)))))
    dataFlow.sc.stop()
  }
}
