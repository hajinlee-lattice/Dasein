package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.query.AggregateLookup
import com.latticeengines.domain.exposed.spark.common.GroupingUtilConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

private[spark] object GroupingUtil {
  def getGroupedDf(df: DataFrame, config: GroupingUtilConfig): DataFrame = {
    df.where(config.getSparkSqlWhereClause)
      .groupBy(config.getGroupKey)
      .agg((config.getAggregateLookup.getAggregator match {
        case AggregateLookup.Aggregator.COUNT => count(config.getAggregationColumn)
        case AggregateLookup.Aggregator.AVG => avg(config.getAggregationColumn)
        case AggregateLookup.Aggregator.MAX => max(config.getAggregationColumn)
        case AggregateLookup.Aggregator.MIN => min(config.getAggregationColumn)
        case AggregateLookup.Aggregator.SUM => sum(config.getAggregationColumn)
        case _ => throw new UnsupportedOperationException("Operation " + config.getAggregateLookup.getAggregator.name + " is not supported for grouping")
      }).as(config.getAggregateLookup.getAlias))
  }
}
