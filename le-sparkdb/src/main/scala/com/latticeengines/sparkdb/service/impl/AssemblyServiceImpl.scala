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
import com.latticeengines.sparkdb.conversion.Implicits._


@Component("assemblyService")
class AssemblyServiceImpl extends AssemblyService {

  private var local = false

  override def run() = {
    val conf = new Configuration()
    val dataFlow = new DataFlow("AvroTest", conf, local)
    try {
      val lead = new AvroSourceTable(dataFlow)
      lead.setPropertyValue(AvroSourceTable.DataPath, "/tmp/sources/Lead_03-10-2014.avro")
      lead.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val opportunity = new AvroSourceTable(dataFlow)
      opportunity.setPropertyValue(AvroSourceTable.DataPath, "/tmp/sources/Opportunity_03-10-2014.avro")
      opportunity.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val contact = new AvroSourceTable(dataFlow)
      contact.setPropertyValue(AvroSourceTable.DataPath, "/tmp/sources/Contact_03-10-2014.avro")
      contact.setPropertyValue(AvroSourceTable.UniqueKeyCol, "Id")

      val filter = new Filter(dataFlow)

      val join = new Join(dataFlow)

      val profiler = new DataProfileOperator(dataFlow)

      val trainingSampler = new Sampler(dataFlow)
      trainingSampler.setPropertyValue(Sampler.WithReplacement, false)
      trainingSampler.setPropertyValue(Sampler.SamplingRate, 0.80)
      val testSampler = new Sampler(dataFlow)
      testSampler.setPropertyValue(Sampler.WithReplacement, false)
      testSampler.setPropertyValue(Sampler.SamplingRate, 0.20)
      
      val filtered = filter.run(join.run(Array(lead.run(), opportunity.run())))
      
      val training = new AvroTargetTable(dataFlow)
      training.setPropertyValue(AvroTargetTable.DataPath, "/tmp/training")
      training.setPropertyValue(AvroTargetTable.ParquetFile, true)

      val test = new AvroTargetTable(dataFlow)
      test.setPropertyValue(AvroTargetTable.DataPath, "/tmp/test")
      test.setPropertyValue(AvroTargetTable.ParquetFile, true)

      training.run(trainingSampler.run(filtered))
      test.run(testSampler.run(filtered))
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