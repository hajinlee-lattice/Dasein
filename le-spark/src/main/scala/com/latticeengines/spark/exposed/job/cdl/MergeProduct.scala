package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.metadata.transaction.{ProductStatus, ProductType}
import com.latticeengines.domain.exposed.spark.cdl.{MergeProductConfig, MergeProductReport}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeProductUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class MergeProduct extends AbstractSparkJob[MergeProductConfig] {

    override def runJob(spark: SparkSession, lattice: LatticeContext[MergeProductConfig]): Unit = {
        val newProds = lattice.input.head
        val oldProdsOpt: Option[DataFrame] = lattice.input.lift(1)

        val (validNew, err1) = validate(newProds, false)
        val validVdb = extractVdbBundleMembers(validNew)
        val (validNewBundle, err2) = filterNewBundleMembers(validNew)

        val newProdsWithBundleId = validNewBundle union validVdb
        val newAnalytic = newProdsWithBundleId
                .withColumn(Description, col(BundleDescription))
                .withColumn(Id, col(BundleId))
                .select(Id, BundleId, Description, Bundle, Priority, Status)
        val allAnalytic = oldProdsOpt match {
            case Some(oldProds) => filterOldAnalytic(oldProds) union newAnalytic
            case _ => newAnalytic
        }
        val (mergedAnalytic, err3) = mergeAnalytic(allAnalytic)
        val newBundleInfo = updateBundleInfo(validNewBundle, mergedAnalytic)

        val (newProdsWithHierarchyIds, err4) = filterNewHierarchyMembers(validNew)
        val (newSpendings, err5) = mergeSpendings(newProdsWithHierarchyIds, oldProdsOpt)

        val analytic = finalizeAnalytic(mergedAnalytic).persist(StorageLevel.DISK_ONLY)
        val bundles = finalizeBundle(newBundleInfo).persist(StorageLevel.DISK_ONLY)
        val spendings = finalizeSpendings(newSpendings).persist(StorageLevel.DISK_ONLY)
        val hierarchies = finalizeHierarchy(newProdsWithHierarchyIds).persist(StorageLevel.DISK_ONLY)
        val products = analytic union bundles union spendings union hierarchies
        val errs: List[String] = (err1 union err2 union err3 union err4 union err5)
          .limit(100).collect().map(r => r.getString(0)).toList

        val report: MergeProductReport = new MergeProductReport
        val numNewProds = newProds.count
        val numActiveAnalytic = analytic.filter(col(Status) === ProductStatus.Active.name).count
        val numActiveSpending = spendings.filter(col(Status) === ProductStatus.Active.name).count
        report.setRecords(numNewProds)
        report.setBundleProducts(bundles.count)
        report.setHierarchyProducts(hierarchies.count)
        report.setAnalyticProducts(numActiveAnalytic)
        report.setSpendingProducts(numActiveSpending)
        report.setInvalidRecords(report.getRecords - validNew.count)
        report.setErrors(errs.asJava)

        lattice.outputStr = serializeJson(report)
        lattice.output = products :: Nil
    }

    private def finalizeBundle(df: DataFrame): DataFrame = {
        alignFinalSchema(
            df.withColumn(Type, lit(ProductType.Bundle.name))
                    .withColumn(Status, lit(ProductStatus.Active.name))
                    .withColumn(Line, lit(null).cast(StringType))
                    .withColumn(Family, lit(null).cast(StringType))
                    .withColumn(Category, lit(null).cast(StringType))
                    .withColumn(LineId, lit(null).cast(StringType))
                    .withColumn(FamilyId, lit(null).cast(StringType))
                    .withColumn(CategoryId, lit(null).cast(StringType))
        )
    }

    private def finalizeSpendings(df: DataFrame): DataFrame = {
        alignFinalSchema(
            df.withColumn(Type, lit(ProductType.Spending.name))
                    .withColumn(Description, lit(null).cast(StringType))
                    .withColumn(Bundle, lit(null).cast(StringType))
                    .withColumn(BundleId, lit(null).cast(StringType))
        )
    }

    private def finalizeHierarchy(df: DataFrame): DataFrame = {
        alignFinalSchema(
            df.withColumn(Type, lit(ProductType.Hierarchy.name))
                    .withColumn(Status, lit(ProductStatus.Active.name))
                    .withColumn(Bundle, lit(null).cast(StringType))
                    .withColumn(BundleId, lit(null).cast(StringType))
        )
    }

}
