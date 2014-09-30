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
      val lead = new AvroSourceTable(dataFlow)
      lead.setPropertyValue(AvroSourceTable.DataPath, "/tmp/Lead/Lead_30-09-2014.avro")
      lead.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val opportunity = new AvroSourceTable(dataFlow)
      opportunity.setPropertyValue(AvroSourceTable.DataPath, "/tmp/Opportunity/Opportunity_30-09-2014.avro")
      opportunity.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val contact = new AvroSourceTable(dataFlow)
      contact.setPropertyValue(AvroSourceTable.DataPath, "/tmp/Contact/Contact_30-09-2014.avro")
      contact.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val filter = new Filter(dataFlow)

      val join = new Join(dataFlow)

      val profiler = new DataProfileOperator(dataFlow)

      val target = new AvroTargetTable(dataFlow)
      target.setPropertyValue(AvroTargetTable.DataPath, "/tmp/result")

      //profiler.run(filter.run(join.run(Array(source1.run(null), source2.run(null)))))
      target.run(filter.run(join.run(Array(lead.run(null), opportunity.run(null)))))
      //target.run(source3.run(null))
    } finally {
      dataFlow.sc.stop()
    }
  }
}

object AssemblyServiceImpl extends App {

  override def main(args: Array[String]) = {
    val assemblyService = new AssemblyServiceImpl()
    assemblyService.local = true
    


    assemblyService.run()
  }

}