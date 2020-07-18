package com.latticeengines.spark.exposed.job.cdl

import java.util

import com.latticeengines.domain.exposed.cdl.activity.AtlasStream.StreamType
import com.latticeengines.domain.exposed.cdl.activity.EventFieldExtractor.MappingType
import com.latticeengines.domain.exposed.cdl.activity.{EventFieldExtractor, TimeLine}
import com.latticeengines.domain.exposed.metadata.InterfaceName.{AccountId, CDLTemplateName, ContactId, ContactName,
  Detail2, Detail1, PathPatternId, PathPattern, PathPatternName}
import com.latticeengines.domain.exposed.spark.cdl.TimeLineJobConfig
import com.latticeengines.domain.exposed.util.{ActivityStoreUtils, TimeLineStoreUtils}
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils.TimelineStandardColumn
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.MergeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.immutable

class TimeLineJob extends AbstractSparkJob[TimeLineJobConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[TimeLineJobConfig]): Unit = {
    //define var
    val config: TimeLineJobConfig = lattice.config
    val inputIdx = config.rawStreamInputIdx.asScala
    val timelineRelatedStreamTables = config.timelineRelatedStreamTables.asScala.mapValues(_.asScala)
    val timelineMap = config.timeLineMap.asScala
    val timelineVersionMap = config.timelineVersionMap.asScala
    val partitionKey: String = config.partitionKey
    val sortKey: String = config.sortKey
    val needRebuild: Boolean = config.needRebuild
    val masterStoreInputIdx = config.masterStoreInputIdx.asScala
    val timelineRelatedMasterTables = config.timelineRelatedMasterTables.asScala
    val metadataMap = config.dimensionMetadataMap.asScala.mapValues(_.asScala)
    val suffix: String = config.tableRoleSuffix
    val contactTable: DataFrame =
      if (config.contactTableIdx != null) {
        lattice.input(config.contactTableIdx)
      } else {
        null
      }
    val generateId = udf {
      () => TimeLineStoreUtils.generateRecordId()
    }

    val generateSortKey = udf {
      (eventTimeStamp: Long, recordId: String) => TimeLineStoreUtils.generateSortKey(eventTimeStamp, recordId)
    }

    val getSourceColumn = udf {
      val templateToSystemTypeMap = config.templateToSystemTypeMap.asScala
      templateName: String => templateToSystemTypeMap.get(templateName)
    }

    //add recordId for every record
    val streamTables = immutable.Map(inputIdx
      .map { case (streamTableName, idx) =>
        val streamTable: DataFrame = {
          val origin: DataFrame = lattice.input(idx)
          val recordIdColumn = TimelineStandardColumn.RecordId.getColumnName
          val sourceColumn = TimelineStandardColumn.TrackedBySystem.getColumnName
          val sourceColumnType = TimelineStandardColumn.TrackedBySystem.getDataType
          val originColumns = origin.columns
          val originWithId = if (!originColumns.contains(recordIdColumn)) {
            origin.withColumn(TimelineStandardColumn.RecordId.getColumnName, generateId())
          } else {
            origin
          }
          if (!originColumns.contains(CDLTemplateName.name())) {
            originWithId.withColumn(sourceColumn, lit(null).cast(sourceColumnType))
          } else {
            originWithId.withColumn(sourceColumn, getSourceColumn(originWithId.col(CDLTemplateName.name())))
          }
        }
        (streamTableName, streamTable)
      }.toSeq: _*)
    //timelineId -> table
    val timelineRawStreamTableMap = immutable.Map(timelineRelatedStreamTables
      .map {
        case (timelineId, entityTableMap) =>
          val timelineObj = timelineMap.getOrElse(timelineId, null)
          val timelineVersion = timelineVersionMap.getOrElse(timelineId, "")
          val recordIdColumn = TimelineStandardColumn.RecordId.getColumnName
          val eventTimeStampColumn = TimelineStandardColumn.EventDate.getColumnName
          val entityIdColumnName = if (timelineObj.getEntity.equalsIgnoreCase("account")) {
            AccountId.name()
          } else {
            ContactId.name()
          }
          var timelineRawStreamTable: DataFrame = createTimelineRawStreamTable(entityTableMap.toMap, streamTables,
            timelineObj, contactTable, lattice)
          val generatePartitionKey = udf {
                val version = timelineVersion
                val id = timelineId
            entityId: String => TimeLineStoreUtils
              .generatePartitionKey(version, id, entityId)
          }
          timelineRawStreamTable = timelineRawStreamTable.withColumn(partitionKey, generatePartitionKey
          (timelineRawStreamTable.col(entityIdColumnName)))
          timelineRawStreamTable = timelineRawStreamTable.withColumn(sortKey, generateSortKey
          (timelineRawStreamTable.col(eventTimeStampColumn), timelineRawStreamTable.col(recordIdColumn)))
          (timelineId, timelineRawStreamTable)
      }.toSeq: _*)
    val timelineMasterStoreMap =
      immutable.Map(timelineRawStreamTableMap.map {
        case (timelineId, timelineRawStreamTable) =>
          val roleTimelineId = timelineId + suffix
          if (!needRebuild) {
            val masterStoreTableName = timelineRelatedMasterTables.getOrElse(timelineId, "")
            val idx: Integer = masterStoreInputIdx.getOrElse(masterStoreTableName, -1)
            val masterStoreTable: DataFrame = lattice.input(idx)
            val mergedMasterTable = MergeUtils.concat2(masterStoreTable, timelineRawStreamTable)
            (roleTimelineId, mergedMasterTable)
          }else {
            (roleTimelineId, timelineRawStreamTable)
          }
      }.toSeq: _*)
    val outputs = (timelineRawStreamTableMap++ timelineMasterStoreMap).toList
    //output
    lattice.output = outputs.map(_._2)
    // timelineId -> corresponding output index
    lattice.outputStr = Serialization.write(outputs.zipWithIndex.map(t => (t._1._1, t._2)).toMap)(org.json4s.DefaultFormats)
  }

  def createTimelineRawStreamTable(entityTableMap: immutable.Map[String, util.Set[String]], streamTables: immutable
  .Map[String, DataFrame], timelineObj: TimeLine, contactTable: DataFrame, lattice: LatticeContext[TimeLineJobConfig])
  : DataFrame = {
    val timelineEntity = timelineObj.getEntity
    val unMergedTimeLineRawStreamTablesByEntity = entityTableMap.map {
      case (entity, timelineRelatedTableNames) =>
        val timelineRelatedStreamTables = streamTables.filter(entry => timelineRelatedTableNames.contains(entry._1))
        //when timeline is account, stream entity is contact, we need join catact batchstore to get accountId
        val tableDf = if (!timelineEntity.equalsIgnoreCase(entity) && entity.equalsIgnoreCase("contact")) {
          timelineRelatedStreamTables.map {
            case (streamTableName, table) =>
              val withContactTable = if (contactTable != null) {
                var shapeTableDf = table
                val tableColumnNames = table.columns
                if (tableColumnNames.contains(AccountId.name())) {
                  shapeTableDf = table.drop(AccountId.name())
                }
                if (tableColumnNames.contains(ContactName.name())) {
                  shapeTableDf = shapeTableDf.drop(ContactName.name())
                }
                //contactId
                shapeTableDf.join(contactTable.select(ContactId.name, AccountId.name(), ContactName
                  .name()), Seq(ContactId.name))
              } else {
                table
              }
              (streamTableName, withContactTable)
          }
        } else {
          timelineRelatedStreamTables
        }
        formatRawStreamTables(timelineRelatedTableNames, tableDf, timelineObj, lattice)
    }
    unMergedTimeLineRawStreamTablesByEntity.reduceLeft((ldf: DataFrame, rdf: DataFrame) => MergeUtils
      .concat2(ldf, rdf))
  }

  def formatRawStreamTables(timelineRelatedTableNames: util.Set[String], streamTables: immutable
  .Map[String, DataFrame], timelineObj: TimeLine, lattice: LatticeContext[TimeLineJobConfig]): DataFrame
  = {
    val formatedRawStreamTables: Stream[DataFrame] = timelineRelatedTableNames.asScala.map(table =>
      formatRawStreamTable(table, streamTables, timelineObj, lattice))
      .toStream
    val mergedFormatedRawStreamTable: DataFrame = formatedRawStreamTables.reduceLeft((ldf: DataFrame, rdf: DataFrame) =>
      MergeUtils.concat2(ldf, rdf))
    mergedFormatedRawStreamTable
  }

  def formatRawStreamTable(timelineRelatedTableName: String, streamTables: immutable
  .Map[String, DataFrame], timelineObj: TimeLine,
                           lattice: LatticeContext[TimeLineJobConfig])
  : DataFrame = {
    val streamTypeWithTableNameMap = lattice.config.streamTypeWithTableNameMap.asScala
    val tableNameToStreamIdMap = lattice.config.tableNameToStreamIdMap.asScala
    val timelineRelatedTable = streamTables(timelineRelatedTableName)
    val allRequiredColumn = TimelineStandardColumn.getColumnNames
    val streamType = streamTypeWithTableNameMap(timelineRelatedTableName)
    val streamId = tableNameToStreamIdMap(timelineRelatedTableName)

    val formatRawStreamTable = allRequiredColumn.asScala.foldLeft(timelineRelatedTable) {
      (df, columnName) =>
        val columnMapping = timelineObj.getEventMappings.get(streamType).get(columnName)
        val formatedRawStreamTable: DataFrame = addAllNullsIfMissing(df, columnName, columnMapping,
          TimelineStandardColumn.getDataTypeFromColumnName(columnName), streamType, streamId, lattice)
        formatedRawStreamTable
    }
    val filterDf = formatRawStreamTable.select(TimelineStandardColumn.getColumnNames.asScala.map(columnName =>
      formatRawStreamTable.col(columnName)):_*)
    logSpark("already filter DataFrame with required columns")
    populateProductPatternNames(filterDf, streamId, streamType, lattice)
  }

  def addAllNullsIfMissing(df: DataFrame, requiredCol: String, mapping: EventFieldExtractor,
                           colType: String, streamType: String, streamId: String, lattice: LatticeContext[TimeLineJobConfig]): DataFrame = {
    val dfColumnNames = df.columns
    if (mapping != null) {
      val mappingValue = mapping.getMappingValue
      mapping.getMappingType match {
        case MappingType.Constant =>
          return df.withColumn(requiredCol, lit(mapping.getMappingValue))
        case MappingType.Attribute =>
          if (dfColumnNames.contains(mappingValue)) {
            return df.withColumn(requiredCol, df.col(mappingValue))
          }
        case MappingType.AttributeWithMapping =>
          if (dfColumnNames.contains(mappingValue)) {
            val mapValue = udf((ts: String) => mapping.getMappingMap.get(ts))
            return df.withColumn(requiredCol, when(df.col(mappingValue).isNotNull, mapValue(df.col(mappingValue))))
          }
      }
    }
    val dfColumnNameMaps = dfColumnNames.map(columnName => (columnName.toLowerCase, columnName)).toMap
    if (!dfColumnNameMaps.contains(requiredCol.toLowerCase)) {
      if (requiredCol.toLowerCase.equals(TimelineStandardColumn.StreamType.getColumnName.toLowerCase)) {
        return df.withColumn(requiredCol, lit(streamType))
      }
      return df.withColumn(requiredCol, lit(null).cast(colType))
    }
    //solve contactId in timeline but ContactId in rawStreamTable
    if (!requiredCol.equals(dfColumnNameMaps.getOrElse(requiredCol.toLowerCase, ""))) {
      return df.withColumnRenamed(dfColumnNameMaps.getOrElse(requiredCol.toLowerCase, ""), requiredCol)
    }
    df
  }

  // This is a method to populate product path names for web visit activity data
  def populateProductPatternNames(df: DataFrame, streamId: String, streamType: String,
  lattice: LatticeContext[TimeLineJobConfig]): DataFrame = {
    if (!StreamType.WebVisit.name().equals(streamType) || streamId == null || streamId.isEmpty) {
      return df
    }
    val metadataMap = lattice.config.dimensionMetadataMap.asScala.mapValues(_.asScala)
    val metadataInStream = metadataMap(streamId)
    if (metadataInStream.isEmpty || !metadataInStream.contains(PathPatternId.name()) || metadataInStream.get
    (PathPatternId.name()).isEmpty) {
      return df
    }

    val dimValues = metadataInStream(PathPatternId.name()).getDimensionValues.asScala.map(_.asScala.toMap).toList
    val pathPatternMap = immutable.Map(dimValues.map(p => (ActivityStoreUtils.modifyPattern(p(PathPattern
      .name()).asInstanceOf[String]).r.pattern, p(PathPatternName.name()).asInstanceOf[String])):_*)
    val filterFn = udf((detailString1: String, detailString2: String)
    => {
      val pathNameString = pathPatternMap.filter(_._1.matcher(detailString1).matches).map(_._2.toString).mkString(",")
      if (pathNameString == null || pathNameString.isEmpty) {
        detailString2
      } else if (detailString2 == null || detailString2.isEmpty) {
        pathNameString
      } else {
        pathNameString + "," + detailString2
      }

    })
    df.withColumn(Detail2.name(), when(df.col(Detail1.name()).isNotNull, filterFn(df.col(Detail1.name())
      , df.col(Detail2.name()))))
    logSpark("----- BEGIN SCRIPT OUTPUT -----")
    df.printSchema
    logSpark("----- END SCRIPT OUTPUT -----")
    df
  }
}

