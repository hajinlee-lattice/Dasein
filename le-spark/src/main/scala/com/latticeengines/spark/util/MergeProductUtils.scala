package com.latticeengines.spark.util

import com.latticeengines.common.exposed.util.{AvroUtils, HashUtils}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.metadata.transaction.{ProductStatus, ProductType}
import com.latticeengines.domain.exposed.util.ProductUtils
import com.latticeengines.spark.aggregation.{DedupBundleProductAggregation, DedupHierarchyProductAggregation, MergeAnalyticProductAggregation, MergeSpendingProductAggregation}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{coalesce, col, explode, lit, udf, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

private[spark] object MergeProductUtils {
  val Id = InterfaceName.Id.name
  val ProductId = InterfaceName.ProductId.name
  val Name = InterfaceName.ProductName.name
  val Description = InterfaceName.Description.name
  val Type = InterfaceName.ProductType.name
  val Bundle = InterfaceName.ProductBundle.name
  val Line = InterfaceName.ProductLine.name
  val Family = InterfaceName.ProductFamily.name
  val Category = InterfaceName.ProductCategory.name
  val BundleId = InterfaceName.ProductBundleId.name
  val LineId = InterfaceName.ProductLineId.name
  val FamilyId = InterfaceName.ProductFamilyId.name
  val CategoryId = InterfaceName.ProductCategoryId.name
  val Status = InterfaceName.ProductStatus.name

  private val Message = "Message"
  val Priority = "Priority"
  val BundleDescription = "BundleDescription"

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

  def validate(prods: DataFrame, checkProductName : Boolean): (DataFrame, DataFrame) = {
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
        if (checkProductName && StringUtils.isBlank(r.getString(nameIdx))) {
          msgs = s"product name is required for product ${r.get(idIdx)} in the upload file." :: msgs
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
  def extractVdbBundleMembers(prods: DataFrame): DataFrame = {
    val filtered = prods.filter(col(Bundle).isNull && col(Category).isNull)
    prods.filter(col(Bundle).isNull && col(Category).isNull)
      .withColumn(Bundle, col(Name)) // use product name as bundle name
      .withColumn(Status, lit(ProductStatus.Active.name))
      .withColumn(BundleDescription, col(Description))
      .withColumn(BundleId, col(Id))
      .withColumn(Priority, lit(2).cast(IntegerType))
      .select(Id, Name, Description, Bundle, BundleId, BundleDescription, Priority, Status)
  }

  def filterNewBundleMembers(prods: DataFrame): (DataFrame, DataFrame) = {
    val idFunc: String => String = name => {
      val compositeId = ProductUtils.getCompositeId(ProductType.Analytic.name, null, name, null, null, null, null)
      HashUtils.getCleanedString(HashUtils.getShortHash(compositeId))
    }
    val idUdf = udf(idFunc)
    val aggregation = new DedupBundleProductAggregation()
    val groupBy = prods.filter(col(Bundle).isNotNull)
      .groupBy(Id, Bundle).agg(aggregation(col(Id), col(Name), col(Description), col(Bundle)).as("agg"))
      .persist(StorageLevel.DISK_ONLY)
    val err = groupBy.select(col("agg.Messages"))
      .withColumn(Message, explode(col("Messages"))).select(Message)
    val valid = groupBy
      .withColumn(Name, col(s"agg.$Name"))
      .withColumn(Description, col(s"agg.$Description"))
      .withColumn(Bundle, col(s"agg.$Bundle"))
      .withColumn(Status, lit(ProductStatus.Active.name))
      .withColumn(BundleDescription, lit(literal = null).cast(StringType))
      .withColumn(BundleId, idUdf(col(Bundle)))
      .withColumn(Priority, lit(1).cast(IntegerType))
      .select(Id, Name, Description, Bundle, BundleId, BundleDescription, Priority, Status)
    (valid, err)
  }

  // ProductBundle is not null, then use it as the Bundle product's name
  def filterOldAnalytic(prods: DataFrame): DataFrame = {
    val filtered = prods.filter(col(Type) === ProductType.Analytic.name)
    val withId = if (filtered.columns.contains(Id) && filtered.columns.contains(ProductId)) {
      filtered.withColumn(Id, coalesce(col(Id), col(ProductId)))
    } else if (filtered.columns.contains(ProductId)) {
      filtered.withColumnRenamed(ProductId, Id)
    } else {
      filtered
    }
    withId
      .withColumn(Priority, lit(3).cast(IntegerType))
      .withColumn(Status, lit(ProductStatus.Obsolete.name))
      .select(Id, BundleId, Description, Bundle, Priority, Status)
  }

  def mergeAnalytic(prods: DataFrame): (DataFrame, DataFrame) = {
    val aggregation = new MergeAnalyticProductAggregation()
    val groupBy = prods.groupBy(BundleId)
      .agg(aggregation(col(Id), col(Bundle), col(Description), col(Status), col(Priority)).as("agg"))
      .withColumn(Id, col(s"agg.$Id"))
      .withColumn(Bundle, col(s"agg.$Bundle"))
      .withColumn(Description, col(s"agg.$Description"))
      .withColumn(Status, col(s"agg.$Status"))
      .persist(StorageLevel.DISK_ONLY)
    val bundles = groupBy.select(Id, BundleId, Description, Bundle, Status)
    val err = groupBy.select(col("agg.Messages"))
      .withColumn(Message, explode(col("Messages"))).select(Message)
    (bundles, err)
  }

  def updateBundleInfo(prods: DataFrame, analyticProds: DataFrame): DataFrame = {
    prods.alias("lhs").join(analyticProds.alias("rhs"), Seq(BundleId), joinType = "left")
      .select(
        col(s"lhs.$Id").as(ProductId),
        col(s"lhs.$Name"),
        col(s"lhs.$Description"),
        col(BundleId),
        col(s"lhs.$Bundle")
      )
  }

  def filterNewHierarchyMembers(prods: DataFrame): (DataFrame, DataFrame) = {
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
      .persist(StorageLevel.DISK_ONLY)
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

  def mergeSpendings(prods: DataFrame, oldProdsOpt: Option[DataFrame]): (DataFrame, DataFrame) = {
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
      .persist(StorageLevel.DISK_ONLY)
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

  def finalizeAnalytic(df: DataFrame): DataFrame = {
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

  def alignFinalSchema(df: DataFrame): DataFrame = {
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

  private def errorSchema(): StructType = {
    StructType(List(
      StructField(Message, StringType)
    ))
  }

}
