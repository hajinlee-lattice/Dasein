package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.CreateEventTableFilterJobConfig
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{lit, col, when}

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._

class CreateEventTableFilterJob extends AbstractSparkJob[CreateEventTableFilterJobConfig] {
    private val accountId = InterfaceName.AccountId.name
    private val periodId = InterfaceName.PeriodId.name
    private val revenueColumn = InterfaceName.__Revenue.name
    private val trainColumn = InterfaceName.Train.name
    
    override def runJob(spark: SparkSession, lattice: LatticeContext[CreateEventTableFilterJobConfig]): Unit = {

      val config: CreateEventTableFilterJobConfig = lattice.config
      var trainFilterTable: DataFrame = lattice.input.head
      var eventFilterTable: DataFrame = lattice.input(1)

      if (eventFilterTable.columns.contains("revenue")) {
        eventFilterTable = eventFilterTable.withColumnRenamed("revenue", revenueColumn)
      }
      val retainFields = ListBuffer[String]()
      retainFields ++= eventFilterTable.columns
      
      val eventColumn = config.getEventColumn
      retainFields += eventColumn
      retainFields += trainColumn 

      trainFilterTable = trainFilterTable.withColumn(trainColumn, when(lit(true), 1))
      eventFilterTable = eventFilterTable.withColumn(eventColumn, when(lit(true), 1))
      var result = trainFilterTable.join(eventFilterTable, Seq(accountId, periodId), joinType = "left")
      result = result.na.fill(0, Seq(eventColumn))
      
      if (eventFilterTable.columns.contains(revenueColumn)) {
          result = result.na.fill(0, Seq(revenueColumn))
      }
      println("----- BEGIN SCRIPT OUTPUT -----")
      println(s"retainFields is: $retainFields")
      println("----- END SCRIPT OUTPUT -----")
      result = result.select(retainFields map col: _*)
      lattice.output = result::Nil
    }
    
}
