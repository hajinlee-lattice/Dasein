package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.serviceflows.core.spark.CreateCdlEventTableJobConfig
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConverters._

class CreateCdlEventTableJob extends AbstractSparkJob[CreateCdlEventTableJobConfig] {
    private val accountId = InterfaceName.AccountId.name
    private val leAccountId = InterfaceName.LEAccount_ID.name
    private val periodId = InterfaceName.PeriodId.name
    
    override def runJob(spark: SparkSession, lattice: LatticeContext[CreateCdlEventTableJobConfig]): Unit = {

      val config: CreateCdlEventTableJobConfig = lattice.config
      
      val inputTable: DataFrame = lattice.input.head
      var accountTable: DataFrame = lattice.input(1)
      var apsTable: DataFrame = if (lattice.input.size > 2) lattice.input(2) else null 
      
      accountTable = processAccountTable(accountTable)
      val retainFields = buildRetainFields(config.eventColumn, inputTable, accountTable, apsTable)
      accountTable = dropDuplicateColumns(inputTable, accountTable, List(accountId))
      var result = inputTable.join(accountTable, Seq(accountId), joinType = "left")
      
      if (apsTable != null) {
        result = dropDuplicateColumns(apsTable, result, 
            List(accountId, periodId, leAccountId, InterfaceName.Period_ID.name))
        result = apsTable.join(
            result, 
            apsTable(leAccountId) === result(accountId) && apsTable(InterfaceName.Period_ID.name) === result(periodId),
            joinType = "right")
      }
      result = result.select(retainFields map col: _*)
      lattice.output = result::Nil
    }
    
    def dropDuplicateColumns(first: DataFrame, second: DataFrame, ids: List[String]) : DataFrame = {
      var result = second
      var dropFields : ListBuffer[String] = ListBuffer()
      result.columns.foreach(c => {
          if (first.columns.contains(c) && !ids.contains(c)) {
              dropFields += c
          }
      })
      if (dropFields.size > 0) {   
        result = result.drop(dropFields:_*)
      }
      result
    }
    
    def processAccountTable(accountTable: DataFrame) : DataFrame = {
      val columns = accountTable.columns
      var resultDf = accountTable
      if (!columns.contains(InterfaceName.CompanyName.name)) {
        breakable {
            for (field <- columns) {
                if (field.equalsIgnoreCase("name")) {
                  resultDf = resultDf.withColumnRenamed(field, InterfaceName.CompanyName.name)
                  break
                }
            }
        }
      }
      resultDf
    }
    
    def buildRetainFields(eventColumn: String, inputTable: DataFrame, accountTable: DataFrame, apsTable: DataFrame) : List[String] = {
        val retainFields = ListBuffer[String]()
        if (apsTable != null) {
            retainFields ++= apsTable.columns
        }
        accountTable.columns.foreach{ attr =>
            if (!retainFields.contains(attr)) {
                retainFields += attr
            }
        }
        val potentialFields = potentialFieldsToRetain(eventColumn) 
        potentialFields.foreach{ attr =>
            if (inputTable.columns.contains(attr) && !retainFields.contains(attr))
                retainFields += attr
        }
        retainFields --= InterfaceName.CDLCreatedTime.name :: InterfaceName.CDLUpdatedTime.name :: Nil
        retainFields.toList
    }

    def potentialFieldsToRetain(eventColumn : String) : List[String] = {
      eventColumn :: InterfaceName.Train.name :: InterfaceName.__Revenue.name :: ScoreResultField.ModelId.displayName ::
                InterfaceName.__Composite_Key__.name :: Nil
    }
    
    
}
