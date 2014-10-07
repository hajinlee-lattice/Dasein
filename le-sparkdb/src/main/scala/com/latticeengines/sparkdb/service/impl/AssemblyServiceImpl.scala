package com.latticeengines.sparkdb.service.impl

import org.apache.avro.Schema.Type
import org.apache.hadoop.conf.Configuration
import org.springframework.stereotype.Component

import com.latticeengines.domain.exposed.eai._
import com.latticeengines.domain.exposed.sparkdb.FunctionExpression

import com.latticeengines.sparkdb.operator.DataFlow
import com.latticeengines.sparkdb.operator.impl.AvroSourceTable
import com.latticeengines.sparkdb.operator.impl.AvroTargetTable
import com.latticeengines.sparkdb.operator.impl.DataProfileOperator
import com.latticeengines.sparkdb.operator.impl.Filter
import com.latticeengines.sparkdb.operator.impl.Join
import com.latticeengines.sparkdb.operator.impl.Sampler
import com.latticeengines.sparkdb.operator.impl.Transform
import com.latticeengines.sparkdb.service.AssemblyService

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
      val isConverted = new Attribute()
      isConverted.setName("IsConverted")
      val filterExpr = new FunctionExpression("com.latticeengines.sparkdb.operator.impl.SampleFunctions$FilterFunction",
        true, null, isConverted)
      filter.setPropertyValue(Filter.FilterCondition, filterExpr)

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

      val transform = new Transform(dataFlow)

      val email = new Attribute()
      email.setName("Email")
      val domain = new Attribute()
      domain.setName("Domain")
      domain.setDisplayName("Domain")
      domain.setPhysicalDataType(Type.STRING.name())
      domain.setLogicalDataType("domain")
      val f1 = new FunctionExpression("com.latticeengines.sparkdb.operator.impl.SampleFunctions$PassThroughFunction",
        true, domain, email)

      val isWon = new Attribute()
      isWon.setName("IsWon")
      isWon.setPhysicalDataType(Type.BOOLEAN.name())
      val isWonDouble = new Attribute()
      isWonDouble.setName("IsWon")
      isWonDouble.setDisplayName("IsWon")
      isWonDouble.setPhysicalDataType(Type.DOUBLE.name())
      isWonDouble.setLogicalDataType("double")
      val f2 = new FunctionExpression("com.latticeengines.sparkdb.operator.impl.SampleFunctions$ConvertBooleanToDouble",
        false, isWonDouble, isWon)
      
      val list = List(f1, f2)
      
      transform.setPropertyValue(Transform.ExpressionList, list)

      val total = new AvroTargetTable(dataFlow)
      total.setPropertyValue(AvroTargetTable.DataPath, "/tmp/result")
      total.setPropertyValue(AvroTargetTable.ParquetFile, true)
      total.run(transform.run(filtered))
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