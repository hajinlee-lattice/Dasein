package com.latticeengines.spark.exposed.job.am

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.spark.am.MapAttributeTxfmrConfig
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.{Map => MMap}
import org.apache.spark.sql.functions.{col, lit}

class MapAttributeJob extends AbstractSparkJob[MapAttributeTxfmrConfig] {
    private val REFRESH_STAGE = "RefreshStage"
    
    override def runJob(spark: SparkSession, lattice: LatticeContext[MapAttributeTxfmrConfig]): Unit = {
        val config: MapAttributeTxfmrConfig = lattice.config
        val inputData: List[DataFrame] = lattice.input
        val sourceMap: MMap[String, DataFrame] = initiateSourceMap(config, inputData)
        val attributes = config.getSrcAttrs.asScala.toList
        val attributeFuncs: List[MapAttributeTxfmrConfig.MapFunc] = 
            if (config.getMapFuncs == null) List() else config.getMapFuncs.asScala.toList
        if (attributeFuncs.size == 0 || sourceMap.size == 0) {
            throw new RuntimeException("Invalid configuration")
        }

        val sourceFuncMap: MMap[String, ListBuffer[MapAttributeTxfmrConfig.MapFunc]] = prepMapsPerSource(attributeFuncs)
        val seedName: String = config.getSeed
        var seed: DataFrame = 
          if (config.getStage.contains(REFRESH_STAGE)) {
            val sourceAttributes: List[SourceAttribute] = attributes
            val newSeed = sourceMap(seedName)
            if (newSeed == null) {
                throw new RuntimeException("Failed to prepare seed " + seedName)
            }
            discardExistingAttrs(newSeed, sourceAttributes, config)
          } else {
              prepareSource(sourceMap(seedName), sourceFuncMap(seedName), null, null,
                      config.getIsDedupe)
          }

        val joinConfigs: List[MapAttributeTxfmrConfig.JoinConfig]  = config.getJoinConfigs.asScala.toList
        val seedId: String = config.getSeedId
        var joined: DataFrame = seed
        for (joinConfig <- joinConfigs) {
            val seedJoinKeys: List[String] = joinConfig.getKeys.asScala.toList
            var sources: ListBuffer[DataFrame] = ListBuffer()
            val joinTargets = joinConfig.getTargets.asScala.toList
            for (joinTarget <- joinTargets) {
                val sourceName = joinTarget.getSource
                if (!sourceMap.contains(sourceName)) {
                  throw new RuntimeException("Failed to prepare source " + sourceName)
                }
                var source: DataFrame = sourceMap(sourceName)
                source = prepareSource(source, sourceFuncMap(sourceName), joinTarget.getKeys.asScala.toList, seedJoinKeys,
                        config.getIsDedupe)
                sources = sources :+ source
            }
            joined = convergeSources(joined, seed, sources.toList, seedId, seedJoinKeys)
        }
        if (StringUtils.isNotBlank(config.getTimestampField)) {
          val stamped: DataFrame = joined.withColumn(config.getTimestampField, lit(System.currentTimeMillis))
          lattice.output = stamped :: Nil
        } else {
          lattice.output = joined :: Nil
        }
    }

    def discardExistingAttrs(df: DataFrame, srcAttrs: List[SourceAttribute], config: MapAttributeTxfmrConfig): DataFrame = {
        val existAttrs: Set[String] = df.columns.toSet
        val timestampField = config.getTimestampField
        var discardAttrs: List[String] = srcAttrs.filter(e => existAttrs.contains(e.getAttribute)).map(_.getAttribute)
        if (StringUtils.isNotBlank(timestampField) && existAttrs.contains(timestampField)) {
            discardAttrs = discardAttrs :+ timestampField
        }
        df.drop(discardAttrs: _*)
    }

    def prepareSource(origSource: DataFrame, mapFuncs: ListBuffer[MapAttributeTxfmrConfig.MapFunc], srcJoinKeys: List[String],
            seedJoinKeys: List[String], isDeduple: Boolean): DataFrame  = {
        var source = origSource
        if (srcJoinKeys != null && true == isDeduple) {
            source = source.dropDuplicates(srcJoinKeys)
        }
        val columnSet: Set[String] = source.columns.toSet
        var sourceAttrs: ListBuffer[String]  = ListBuffer()
        var outputAttrs: ListBuffer[String] = ListBuffer()

        // Filter out missing attributes and join keys
        mapFuncs.filter(e => columnSet.contains(e.getAttribute)).filter(e => srcJoinKeys != null && !srcJoinKeys.contains(e))
        .map(srcAttr => {
          sourceAttrs += srcAttr.getAttribute
          outputAttrs += srcAttr.getTarget
        })
        
        if (srcJoinKeys != null) {
            for (i <- 0 to srcJoinKeys.size-1) {          
              sourceAttrs += srcJoinKeys(i)
              outputAttrs += seedJoinKeys(i)
            }
        }

        val namePairs = sourceAttrs.zip(outputAttrs)
        val renamed = namePairs.foldLeft(source){(df, names) =>
            df.withColumnRenamed(names._1, names._2)
        }
        val retained: DataFrame = renamed.select(outputAttrs map col: _*)
        retained
    }

    def prepMapsPerSource(mapFuncs: List[MapAttributeTxfmrConfig.MapFunc]): MMap[String, ListBuffer[MapAttributeTxfmrConfig.MapFunc]] = {
        var sourceAttributes: MMap[String, ListBuffer[MapAttributeTxfmrConfig.MapFunc]] = MMap()
        for (mapFunc <- mapFuncs) {
            val origSource = mapFunc.getSource
            var attrsPerSrc = 
              if (sourceAttributes.contains(origSource)) {
                sourceAttributes(origSource)
              } else {
                var newAttrsPerSrc: ListBuffer[MapAttributeTxfmrConfig.MapFunc] = ListBuffer()
                sourceAttributes(origSource) = newAttrsPerSrc
                newAttrsPerSrc
              }
            attrsPerSrc += mapFunc
        }
        return sourceAttributes
    }

    def convergeSources(joined: DataFrame, seed: DataFrame, dfs: List[DataFrame], seedId: String, seedJoinKeys: List[String]): DataFrame = {
        if (dfs.size == 0) {
            throw new RuntimeException("There's no source specified!")
        }
        val seedOutputAttrs: ListBuffer[String] = ListBuffer()
        seedOutputAttrs += seedId
        seedOutputAttrs ++= seedJoinKeys

        val filterCond = seedJoinKeys.map(c=>col(c).isNotNull).reduce(_ || _)
        val filteredSeed = seed.filter(filterCond)
        val retainedSeed: DataFrame = filteredSeed.select(seedOutputAttrs map col: _*)
    
        val coGrouped: DataFrame = dfs.foldLeft(retainedSeed){(lhs, rhs) => 
          lhs.transform(nullSafeJoin(rhs, seedJoinKeys, "left"))
        }
        val filtered = coGrouped.drop(seedJoinKeys: _*)
        
        println("----- BEGIN SCRIPT OUTPUT -----")
        println(s"retainedSeed columns is: ${retainedSeed.columns.mkString(",")}")
        println(s"filtered columns is: ${filtered.columns.mkString(",")}")
        println("----- END SCRIPT OUTPUT -----")
        
        joined.join(filtered, Seq(seedId), "left")
    }

    def initiateSourceMap(config: MapAttributeTxfmrConfig, inputData: List[DataFrame]): MMap[String, DataFrame] = {
        val tableNames: List[String] = if (config.getBaseTables == null) List() else config.getBaseTables.asScala.toList
        val sourceNames: List[String] = if (config.getTemplates == null) List() else config.getTemplates.asScala.toList
    
        if (tableNames.size != sourceNames.size) {
            throw new RuntimeException("Source names and tables does not match")
        }
        var sourceMap: MMap[String, DataFrame]  = MMap()
        for (i <- 0 to tableNames.size - 1) {
            sourceMap(sourceNames(i)) = inputData(i)
        }
        return sourceMap
    }
    
    def nullSafeJoin(rightDF: DataFrame, columns: Seq[String], joinType: String)(leftDF: DataFrame): DataFrame = {
        val colExpr: Column = leftDF(columns.head) <=> rightDF(columns.head)
        val fullExpr = columns.tail.foldLeft(colExpr) { 
          (colExpr, p) => colExpr && leftDF(p) <=> rightDF(p) 
        }
        leftDF.join(rightDF, fullExpr, joinType)
    }
}
