package com.latticeengines.sparkdb.service.impl

import org.apache.hadoop.conf.Configuration
import org.springframework.stereotype.Component

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.impl.AvroSourceTable
import com.latticeengines.sparkdb.operator.impl.AvroTargetTable
import com.latticeengines.sparkdb.operator.impl.DataProfileOperator
import com.latticeengines.sparkdb.operator.impl.Filter
import com.latticeengines.sparkdb.operator.impl.Join
import com.latticeengines.sparkdb.operator.impl.Sampler
import com.latticeengines.sparkdb.service.AssemblyService

@Component("assemblyService")
class AssemblyServiceImpl extends AssemblyService {

  private var local = false

  override def run() = {
    val conf = new Configuration()
    val dataFlow = new DataFlow("AvroTest", conf, local)
    try {
      val lead = new AvroSourceTable(dataFlow)
      lead.setPropertyValue(AvroSourceTable.DataPath, "/tmp/Lead/Lead_01-10-2014.avro")
      lead.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val opportunity = new AvroSourceTable(dataFlow)
      opportunity.setPropertyValue(AvroSourceTable.DataPath, "/tmp/Opportunity/Opportunity_01-10-2014.avro")
      opportunity.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val contact = new AvroSourceTable(dataFlow)
      contact.setPropertyValue(AvroSourceTable.DataPath, "/tmp/Contact/Contact_01-10-2014.avro")
      contact.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val filter = new Filter(dataFlow)

      val join = new Join(dataFlow)

      val profiler = new DataProfileOperator(dataFlow)

      val sampler = new Sampler(dataFlow)
      sampler.setPropertyValue(Sampler.WithReplacement, false)
      sampler.setPropertyValue(Sampler.SamplingRate, 0.90)
      
      val target = new AvroTargetTable(dataFlow)
      target.setPropertyValue(AvroTargetTable.DataPath, "/tmp/result")
      target.setPropertyValue(AvroTargetTable.ParquetFile, false)

      //profiler.run(filter.run(join.run(Array(source1.run(null), source2.run(null)))))
      target.run(sampler.run(filter.run(join.run(Array(lead.run(), opportunity.run())))))
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