package com.latticeengines.sparkdb.service.impl

import org.apache.hadoop.conf.Configuration
import org.springframework.stereotype.Component

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.impl.AvroSourceTable
import com.latticeengines.sparkdb.operator.impl.AvroTargetTable
import com.latticeengines.sparkdb.operator.impl.DataProfileOperator
import com.latticeengines.sparkdb.operator.impl.Filter
import com.latticeengines.sparkdb.operator.impl.Join
import com.latticeengines.sparkdb.service.AssemblyService

@Component("assemblyService")
class AssemblyServiceImpl extends AssemblyService {

  private var local = false

  override def run() = {
    val conf = new Configuration()
    val dataFlow = new DataFlow("AvroTest", conf, local)
    try {
      val source1 = new AvroSourceTable(dataFlow)
      source1.setPropertyValue(AvroSourceTable.DataPath, "/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/samples/allTraining-r-00000.avro")
      source1.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Nutanix_EventTable_Clean")

      val source2 = new AvroSourceTable(dataFlow)
      source2.setPropertyValue(AvroSourceTable.DataPath, "/user/s-analytics/customers/Nutanix/data/Q_EventTable_Nutanix/samples/allTraining-r-00000.avro")
      source2.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Nutanix_EventTable_Clean")

      val source3 = new AvroSourceTable(dataFlow)
      source3.setPropertyValue(AvroSourceTable.DataPath, "/tmp/result/part-r-00000.avro")
      source3.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Nutanix_EventTable_Clean")

      val filter = new Filter(dataFlow)

      val join = new Join(dataFlow)

      val profiler = new DataProfileOperator(dataFlow)

      val target = new AvroTargetTable(dataFlow)
      target.setPropertyValue(AvroTargetTable.DataPath, "/tmp/result")

      //profiler.run(filter.run(join.run(Array(source1.run(null), source2.run(null)))))
      target.run(filter.run(join.run(Array(source1.run(null), source2.run(null)))))
      //target.run(source3.run(null))
    } finally {
      dataFlow.sc.stop()
    }
  }
}

object AssemblyServiceImpl extends App {

  override def main(args: Array[String]) = {
    val assemblyService = new AssemblyServiceImpl()

    if (args.length > 0) {
      assemblyService.local = true
    }


    assemblyService.run()
  }

}