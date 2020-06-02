package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.JsonUtils
import com.latticeengines.domain.exposed.pls.ProductValidationSummary
import com.latticeengines.domain.exposed.spark.cdl.ValidateProductConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeProductUtils._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class ValidateProduct extends AbstractSparkJob[ValidateProductConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[ValidateProductConfig]): Unit = {
    val newProds = lattice.input.head
    val oldProdsOpt: Option[DataFrame] = lattice.input.lift(1)
    val config = lattice.config

    val (validNew, err1) = validate(newProds, config.getCheckProductName)
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

    val newRecords = finalizeAnalytic(newAnalytic).persist(StorageLevel.DISK_ONLY)
    val errs = err1.persist(StorageLevel.DISK_ONLY)
    val errsAndWarnings = (errs union  err2 union  err3 union err4 union err5).persist(StorageLevel.DISK_ONLY)
    val productSummary = new ProductValidationSummary
    productSummary.setErrorLineNumber(errs.count())

    lattice.output = newRecords :: errsAndWarnings :: Nil
    lattice.outputStr = JsonUtils.serialize(productSummary)
  }

}
