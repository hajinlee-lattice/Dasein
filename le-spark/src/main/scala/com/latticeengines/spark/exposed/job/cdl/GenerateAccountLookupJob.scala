package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.spark.cdl.GenerateAccountLookupConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class GenerateAccountLookupJob extends AbstractSparkJob[GenerateAccountLookupConfig] {

    private val accountId = InterfaceName.AccountId.name
    private val lookupKey = InterfaceName.AtlasLookupKey.name
    private val customerAccountId = InterfaceName.CustomerAccountId.name

    override def runJob(spark: SparkSession, lattice: LatticeContext[GenerateAccountLookupConfig]): Unit = {
        val config: GenerateAccountLookupConfig = lattice.config
        val input = lattice.input.head
        val byAccountId = input.select(accountId)
          // FIXME remove this tmp workaround after null accountId issue is fixed
          .filter(col(accountId).isNotNull)
          .withColumn(lookupKey, getLookupKeyUdf(accountId)(col(accountId)))

        var lookupIds: Set[String] = if (config.getLookupIds == null) Set() else config.getLookupIds.asScala.toSet
        if (input.columns.contains(customerAccountId)) {
            lookupIds += customerAccountId
        }
        val byLookupIds: Set[DataFrame] = lookupIds.map(lookupId => {
            input.select(accountId, lookupId).filter(col(lookupId).isNotNull) //
                    .withColumn(lookupKey, getLookupKeyUdf(lookupId)(col(lookupId))) //
                    .select(accountId, lookupKey)
        })

        val merged = byLookupIds.fold(byAccountId)(_ union _)
        lattice.output = merged :: Nil
    }

    def getLookupKeyUdf(lookupId: String): UserDefinedFunction = {
        val func: String => String = lookupVal => lookupId + "_" + lookupVal.toLowerCase
        udf(func)
    }

}
