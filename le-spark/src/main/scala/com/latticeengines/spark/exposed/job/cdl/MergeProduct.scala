package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.common.exposed.util.{AvroUtils, HashUtils}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.{ProductStatus, ProductType}
import com.latticeengines.domain.exposed.spark.cdl.{MergeProductConfig, MergeProductReport}
import com.latticeengines.domain.exposed.util.ProductUtils
import com.latticeengines.spark.aggregation.{AnalyticProductAggregation, DedupBundleProductAggregation, DedupHierarchyProductAggregation, MergeSpendingProductAggregation}
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class MergeProduct extends AbstractSparkJob[MergeProductConfig] {

    private val Id = InterfaceName.Id.name
    private val ProductId = InterfaceName.ProductId.name
    private val Name = InterfaceName.ProductName.name
    private val Description = InterfaceName.Description.name
    private val Type = InterfaceName.ProductType.name
    private val Bundle = InterfaceName.ProductBundle.name
    private val Line = InterfaceName.ProductLine.name
    private val Family = InterfaceName.ProductFamily.name
    private val Category = InterfaceName.ProductCategory.name
    private val BundleId = InterfaceName.ProductBundleId.name
    private val LineId = InterfaceName.ProductLineId.name
    private val FamilyId = InterfaceName.ProductFamilyId.name
    private val CategoryId = InterfaceName.ProductCategoryId.name
    private val Status = InterfaceName.ProductStatus.name

    private val Message = "Message"
    private val Priority = "Priority"
    private val BundleDescription = "BundleDescription"

    override def runJob(spark: SparkSession, lattice: LatticeContext[MergeProductConfig]): Unit = {
        val newProds = lattice.input.head
        val oldProdsOpt: Option[DataFrame] = lattice.input.lift(1)

        val (validNew, err1) = validate(newProds)
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

    private def mergeId(input: DataFrame): DataFrame = {
        val withId = if (input.columns.contains(Id) && input.columns.contains(ProductId)) {
            input.withColumn(Id, when(col(Id).isNotNull, col(Id)).otherwise(col(ProductId))).drop(ProductId)
        } else if (input.columns.contains(ProductId)) {
            input.withColumnRenamed(ProductId, Id)
        } else {
            input
        }
        List(Name, Description, Bundle, Line, Family, Category).foldLeft(withId)((prev, c) => {
            if (!prev.columns.contains(c)) {
                prev.withColumn(c, lit(null).cast(StringType))
            } else {
                prev.withColumn(c, when(col(c) === "", null).otherwise(col(c)))
            }
        }).select(Id, Name, Description, Bundle, Line, Family, Category)
    }

    private def validate(prods: DataFrame): (DataFrame, DataFrame) = {
        val idMerged = mergeId(prods)
        val idIdx = idMerged.columns.indexOf(Id)
        val nameIdx = idMerged.columns.indexOf(Name)
        val bundleIdx = idMerged.columns.indexOf(Bundle)
        val lineIdx = idMerged.columns.indexOf(Line)
        val famIdx = idMerged.columns.indexOf(Family)
        val catIdx = idMerged.columns.indexOf(Category)

        val valid = idMerged.filter(r => {
            var isValid = true
            if (r.get(idIdx) == null) {
                isValid = false
            } else if (r.get(bundleIdx) == null && r.get(catIdx) == null) { // vdb products
                if (!AvroUtils.isValidColumn(r.getString(idIdx))) {
                    isValid = false
                } else if (StringUtils.isBlank(r.getString(nameIdx))) {
                    isValid = false
                }
            }
            isValid
        })

        val invalid = idMerged.flatMap(r => {
            var msgs: List[String] = List()
            if (r.get(idIdx) == null) {
                msgs = s"Id cannot be null: ${r.mkString(", ")}" :: msgs
            } else {
                if (r.get(bundleIdx) == null && r.get(catIdx) == null) { // vdb products
                    val id = r.getString(idIdx)
                    if (!AvroUtils.isValidColumn(id)) {
                        msgs = s"Id $id is not compatible to avro schema: $id" :: msgs
                    } else if (StringUtils.isBlank(r.getString(nameIdx))) {
                        msgs = s"VDB product must provide a valid product name: $id" :: msgs
                    }
                }
                if (r.get(lineIdx) != null && r.get(famIdx) == null) {
                    msgs = s"Must provide product family when product line is given: ${r.get(idIdx)}" :: msgs
                }
                if (r.get(famIdx) != null && r.get(catIdx) == null) {
                    msgs = s"Must provide product category when product family is given: ${r.get(idIdx)}" :: msgs
                }
            }
            msgs.map(m => Row.fromSeq(Seq(m)))
        })(RowEncoder(errorSchema()))

        (valid, invalid)
    }

    // VDB has only bundles
    private def extractVdbBundleMembers(prods: DataFrame): DataFrame = {
        val filtered = prods.filter(col(Bundle).isNull && col(Category).isNull)
        prods.filter(col(Bundle).isNull && col(Category).isNull)
                .withColumn(Bundle, col(Name)) // use product name as bundle name
                .withColumn(Status, lit(ProductStatus.Active.name))
                .withColumn(BundleDescription, col(Description))
                .withColumn(BundleId, col(Id))
                .withColumn(Priority, lit(2).cast(IntegerType))
                .select(Id, Name, Description, Bundle, BundleId, BundleDescription, Priority, Status)
    }

    private def errorSchema(): StructType = {
        StructType(List(
            StructField(Message, StringType)
        ))
    }

    private def filterNewBundleMembers(prods: DataFrame): (DataFrame, DataFrame) = {
        val idFunc: String => String = name => {
            val compositeId = ProductUtils.getCompositeId(ProductType.Analytic.name, null, name, null, null, null, null)
            HashUtils.getCleanedString(HashUtils.getShortHash(compositeId))
        }
        val idUdf = udf(idFunc)
        val aggregation = new DedupBundleProductAggregation()
        val groupBy = prods.filter(col(Bundle).isNotNull)
                .groupBy(Id, Bundle).agg(aggregation(col(Id), col(Name), col(Description), col(Bundle)).as("agg"))
        val err = groupBy.select(col("agg.Messages"))
                .withColumn(Message, explode(col("Messages"))).select(Message)
        val valid = groupBy
                .withColumn(Name, col(s"agg.$Name"))
                .withColumn(Description, col(s"agg.$Description"))
                .withColumn(Bundle, col(s"agg.$Bundle"))
                .withColumn(Status, lit(ProductStatus.Active.name))
                .withColumn(BundleDescription, lit(literal = null).cast(StringType))
                .withColumn(BundleId, idUdf(col(Bundle)))
                .withColumn(Priority, lit(3).cast(IntegerType))
                .select(Id, Name, Description, Bundle, BundleId, BundleDescription, Priority, Status)
        (valid, err)
    }

    // ProductBundle is not null, then use it as the Bundle product's name
    private def filterOldAnalytic(prods: DataFrame): DataFrame = {
        prods.filter(col(Type) === ProductType.Analytic.name)
                .withColumnRenamed(ProductId, Id)
                .withColumn(Priority, lit(1).cast(IntegerType))
                .withColumn(Status, lit(ProductStatus.Obsolete.name))
                .select(Id, BundleId, Description, Bundle, Priority, Status)
    }

    private def mergeAnalytic(prods: DataFrame): (DataFrame, DataFrame) = {
        val aggregation = new AnalyticProductAggregation()
        val groupBy = prods.groupBy(BundleId)
                .agg(aggregation(col(Id), col(Bundle), col(Description), col(Status), col(Priority)).as("agg"))
                .withColumn(Id, col(s"agg.$Id"))
                .withColumn(Bundle, col(s"agg.$Bundle"))
                .withColumn(Description, col(s"agg.$Description"))
                .withColumn(Status, col(s"agg.$Status"))
        val bundles = groupBy.select(Id, BundleId, Description, Bundle, Status)
        val err = groupBy.select(col("agg.Messages"))
                .withColumn(Message, explode(col("Messages"))).select(Message)
        (bundles, err)
    }

    private def updateBundleInfo(prods: DataFrame, analyticProds: DataFrame): DataFrame = {
        prods.alias("lhs").join(analyticProds.alias("rhs"), Seq(BundleId), joinType = "left")
                .select(
                    col(s"lhs.$Id").as(ProductId),
                    col(s"lhs.$Name"),
                    col(s"lhs.$Description"),
                    col(BundleId),
                    col(s"lhs.$Bundle")
                )
    }

    private def filterNewHierarchyMembers(prods: DataFrame): (DataFrame, DataFrame) = {
        // udf to generate line id
        val lineIdFunc: (String, String, String) => String = (cat, fam, line) => {
            val compositeId = ProductUtils.getCompositeId(ProductType.Spending.name, null, line, null, cat, fam, line)
            HashUtils.getCleanedString(HashUtils.getShortHash(compositeId))
        }
        val lineIdUdf = udf(lineIdFunc)
        // udf to generate family id
        val famIdFunc: (String, String) => String = (cat, fam) => {
            val compositeId = ProductUtils.getCompositeId(ProductType.Spending.name, null, fam, null, cat, fam, null)
            HashUtils.getCleanedString(HashUtils.getShortHash(compositeId))
        }
        val famIdUdf = udf(famIdFunc)
        // udf to generate category id
        val catIdFunc: String => String = cat => {
            val compositeId = ProductUtils.getCompositeId(ProductType.Spending.name, null, cat, null, cat, null, null)
            HashUtils.getCleanedString(HashUtils.getShortHash(compositeId))
        }
        val catIdUdf = udf(catIdFunc)

        // if contains category, then it is an hierarchy product
        // filter then dedup by id -> one sku should only appear once (otherwise, either duplication or confliction)
        val filtered = prods.filter(col(Category).isNotNull).select(Id, Name, Description, Line, Family, Category)
        val dedup = new DedupHierarchyProductAggregation()
        val groupBy = filtered.groupBy(Id)
                .agg(dedup(col(Id), col(Name), col(Description), col(Line), col(Family), col(Category)).as("agg"))
        val err = groupBy.select(col("agg.Messages"))
                .withColumn(Message, explode(col("Messages"))).select(Message)
        val valid = groupBy
                .withColumn(Name, col(s"agg.$Name"))
                .withColumn(Description, col(s"agg.$Description"))
                .withColumn(Line, col(s"agg.$Line"))
                .withColumn(Family, col(s"agg.$Family"))
                .withColumn(Category, col(s"agg.$Category"))
                .withColumn(ProductId, col(Id))
                .withColumn(LineId, when(col(Line).isNotNull, lineIdUdf(col(Category), col(Family), col(Line))).otherwise(lit(null).cast(StringType)))
                .withColumn(FamilyId, when(col(Family).isNotNull, famIdUdf(col(Category), col(Family))).otherwise(lit(null).cast(StringType)))
                .withColumn(CategoryId, when(col(Category).isNotNull, catIdUdf(col(Category))).otherwise(lit(null).cast(StringType)))
                .select(ProductId, Name, Description, Line, Family, Category, LineId, FamilyId, CategoryId)
        (valid, err)
    }

    private def mergeSpendings(prods: DataFrame, oldProdsOpt: Option[DataFrame]): (DataFrame, DataFrame) = {
        val newSpendings = expandNewSpending(prods).withColumn(Status, lit(ProductStatus.Active.name))
        val allSpendings = oldProdsOpt match {
            case Some(oldProds) =>
                val oldSpendings = oldProds.filter(col(Type) === ProductType.Spending.name)
                        .withColumn(Status, lit(ProductStatus.Obsolete.name))
                        .select(newSpendings.columns.map(col): _*)
                newSpendings union oldSpendings
            case _ => newSpendings
        }
        val aggregation = new MergeSpendingProductAggregation()
        val groupBy = allSpendings.groupBy(ProductId)
                .agg(aggregation(
                    col(Name), col(Line), col(Family), col(Category), col(LineId), col(FamilyId), col(CategoryId), col(Status)
                ).as("agg"))
                .withColumn(Name, col(s"agg.$Name"))
                .withColumn(Line, col(s"agg.$Line"))
                .withColumn(Family, col(s"agg.$Family"))
                .withColumn(Category, col(s"agg.$Category"))
                .withColumn(LineId, col(s"agg.$LineId"))
                .withColumn(FamilyId, col(s"agg.$FamilyId"))
                .withColumn(CategoryId, col(s"agg.$CategoryId"))
                .withColumn(Status, col(s"agg.$Status"))
        val spendings = groupBy.select(ProductId, Name, Line, Family, Category, LineId, FamilyId, CategoryId, Status)
        val err = groupBy.select(col("agg.Messages"))
                .withColumn(Message, explode(col("Messages"))).select(Message)
        (spendings, err)
    }

    private def expandNewSpending(prods: DataFrame): DataFrame = {
        val lines = prods.filter(col(LineId).isNotNull)
                .withColumn(ProductId, col(LineId))
                .withColumn(Name, col(Line))
                .withColumn(LineId, lit(null).cast(StringType))
                .select(ProductId, Name, Line, Family, Category, LineId, FamilyId, CategoryId)
        val fams = prods.filter(col(FamilyId).isNotNull)
                .withColumn(ProductId, col(FamilyId))
                .withColumn(Name, col(Family))
                .withColumn(Line, lit(null).cast(StringType))
                .withColumn(LineId, lit(null).cast(StringType))
                .withColumn(FamilyId, lit(null).cast(StringType))
                .select(ProductId, Name, Line, Family, Category, LineId, FamilyId, CategoryId)
        val cats = prods.filter(col(CategoryId).isNotNull)
                .withColumn(ProductId, col(CategoryId))
                .withColumn(Name, col(Category))
                .withColumn(Line, lit(null).cast(StringType))
                .withColumn(Family, lit(null).cast(StringType))
                .withColumn(LineId, lit(null).cast(StringType))
                .withColumn(FamilyId, lit(null).cast(StringType))
                .withColumn(CategoryId, lit(null).cast(StringType))
                .select(ProductId, Name, Line, Family, Category, LineId, FamilyId, CategoryId)
        lines union fams union cats
    }

    private def finalizeAnalytic(df: DataFrame): DataFrame = {
        alignFinalSchema(
            df.withColumn(Type, lit(ProductType.Analytic.name))
                    .withColumnRenamed(Id, ProductId)
                    .withColumn(Name, col(Bundle))
                    .withColumn(Line, lit(null).cast(StringType))
                    .withColumn(Family, lit(null).cast(StringType))
                    .withColumn(Category, lit(null).cast(StringType))
                    .withColumn(LineId, lit(null).cast(StringType))
                    .withColumn(FamilyId, lit(null).cast(StringType))
                    .withColumn(CategoryId, lit(null).cast(StringType))
        )
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

    private def alignFinalSchema(df: DataFrame): DataFrame = {
        df.select(
            ProductId,
            Name,
            Description,
            Bundle,
            Line,
            Family,
            Category,
            BundleId,
            LineId,
            FamilyId,
            CategoryId,
            Type,
            Status
        )
    }

}
