import org.apache.spark.storage.StorageLevel

val trxnTableName = lattice.params.get("TRXN_TABLE").asText()
val periodName = lattice.params.get("PERIOD_NAME").asText()
val storageLevel: Option[StorageLevel] =
  if (lattice.params.hasNonNull("STORAGE_LEVEL")) {
    Some(StorageLevel.fromString(lattice.params.get("STORAGE_LEVEL").asText()))
  } else {
    None
  }

val tempSqls: List[(String, String)] = List(
  ("temptrxn", s"select AccountId, PeriodId, ProductId, TotalAmount, TotalQuantity from $trxnTableName where PeriodName = '$periodName'"),
  ("crossprod", "select AccountId, PeriodId from ( select distinct AccountId from temptrxn ) as allaccounts, ( select distinct PeriodId from temptrxn ) as allperiods"),
  ("periodrange", "select AccountId, min(PeriodId) as minpid from temptrxn group by AccountId"),
  ("tempkeys", "select crossprod.AccountId, crossprod.PeriodId from crossprod inner join periodrange on periodrange.AccountId = crossprod.AccountId where crossprod.PeriodId >= periodrange.minpid - 2")
)

spark.conf.set("spark.sql.crossJoin.enabled", "true")

tempSqls map {t => {
  val viewName = t._1
  val viewSql = t._2
  println("----- BEGIN SCRIPT OUTPUT -----")
  println(s"\nSQL Statement for $viewName:")
  println(viewSql)
  println("----- END SCRIPT OUTPUT -----")
  0
}}

tempSqls map {t =>
  val viewName = t._1
  val viewSql = t._2
  val viewDf = spark.sql(viewSql)
  storageLevel match {
    case Some(lvl) =>
      viewDf.persist(lvl).createOrReplaceTempView(viewName)
    case _ =>
      viewDf.createOrReplaceTempView(viewName)
  }
}

