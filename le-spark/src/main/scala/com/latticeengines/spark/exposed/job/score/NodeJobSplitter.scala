package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.scoring.ScoreResultField;

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.storage.StorageLevel

object NodeJobSplitter {
  
     def splitEv(input: DataFrame, originalScoreFieldMap:Map[String, String], modelGuidFieldName: String) = {
        val modelList: ListBuffer[String] = ListBuffer()
        val predictedModelList: ListBuffer[String] = ListBuffer()
        val evModelList:  ListBuffer[String] = ListBuffer()
        for ((modelGuid, scoreField) <- originalScoreFieldMap) {
            splitModelNames(originalScoreFieldMap, modelList, predictedModelList, evModelList, modelGuid)
        }
        val model = createNodeInList(input, modelGuidFieldName, modelList.toList)
        val predictedModel = createNodeInList(input, modelGuidFieldName, predictedModelList.toList)
        val evModel = createNodeInList(input, modelGuidFieldName, evModelList.toList)
        (model, predictedModel, evModel)
    }

    def splitModelNames(originalScoreFieldMap: Map[String, String],  modelList: ListBuffer[String],
            predictedModelList:ListBuffer[String], evModelList: ListBuffer[String], modelGuid: String) : Unit = {
        val isNotRevenue: Boolean = (ScoreResultField.RawScore.displayName
                == originalScoreFieldMap.getOrElse(modelGuid, ScoreResultField.RawScore.displayName))
        if (isNotRevenue) {
            modelList += modelGuid
        } else {
            val predicted: Boolean = (ScoreResultField.PredictedRevenue.displayName
                    == originalScoreFieldMap.get(modelGuid))
            if (predicted) {
                predictedModelList += modelGuid
            } else {
                evModelList += modelGuid
            }
        }
    }

    def createNodeInList(input: DataFrame, modelGuidFieldName: String, modelList: List[String]): DataFrame = {
        if (!modelList.isEmpty) {
            val filtered = input.filter(col(modelGuidFieldName).isin(modelList: _*))
            return filtered.persist(StorageLevel.DISK_ONLY)
        }
        return null
    }
    
    def createNodeNotInList(input: DataFrame, modelGuidFieldName: String, modelList: List[String]): DataFrame = {
        if (!modelList.isEmpty) {
            val filtered = input.filter(!col(modelGuidFieldName).isin(modelList: _*))
            return filtered
        }
        return null
    }
}
