package com.latticeengines.spark.exposed.job.score

import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CombineInputTableWithScoreJobConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.domain.exposed.util.BucketMetadataUtils
import com.latticeengines.domain.exposed.scoring.ScoreResultField
import com.latticeengines.domain.exposed.pls.ModelType
import com.latticeengines.domain.exposed.pls.BucketMetadata
import java.util.List

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf, col}
import scala.collection.JavaConverters._

class CombineInputTableWithScoreJob extends AbstractSparkJob[CombineInputTableWithScoreJobConfig] {

    val ratingField = ScoreResultField.Rating.displayName
    
    override def runJob(spark: SparkSession, lattice: LatticeContext[CombineInputTableWithScoreJobConfig]): Unit = {

      val config: CombineInputTableWithScoreJobConfig = lattice.config
      val inputTable: DataFrame = lattice.input.head
      var scoreTable: DataFrame = lattice.input(1)
      
      val noRatingColumnInScoreTable = !scoreTable.columns.contains(ratingField)
      val notPMMLModel = config.modelType == null || config.modelType == ModelType.PYTHONMODEL.getModelType
  
      if (StringUtils.isNotBlank(config.modelIdField)) {
          // "Enter multi-model mode"
          val bucketMetadataMap = config.bucketMetadataMap.asScala.toMap
          val modelIdField = config.modelIdField
          val scoreField = config.scoreFieldName
          val addRatingColumnUdf = udf((score: Double, modelId: String) => {
            BucketMetadataUtils.bucketScore(bucketMetadataMap(modelId), score)
          })
          scoreTable = scoreTable.withColumn(ratingField, addRatingColumnUdf(col(scoreField), col(modelIdField)))
          
      } else if (noRatingColumnInScoreTable && notPMMLModel) {
          // "Enter single-model mode"
          val bucketMetadata = config.bucketMetadata
          val scoreField = config.scoreFieldName
          val addRatingColumnUdf = udf((score: Double) => {
            BucketMetadataUtils.bucketScore(bucketMetadata, score)
          })
          scoreTable = scoreTable.withColumn(ratingField, addRatingColumnUdf(col(scoreField)))
      }
        
      var idColumn = InterfaceName.InternalId.name
      var groupByColumn = InterfaceName.InternalId.name
      if (StringUtils.isNotEmpty(config.idColumn)) {
          idColumn = config.idColumn
          groupByColumn = idColumn
      } else if (inputTable.columns.contains(InterfaceName.InternalId.name)) {
          idColumn = InterfaceName.InternalId.name
      }
  
      var retainFields = inputTable.columns
      var dropFields : scala.collection.immutable.List[String] = scala.collection.immutable.List()
      scoreTable.columns.foreach(c => {
          if (retainFields.contains(c) && c != idColumn) {
              dropFields = c :: dropFields
          }
      })

      if (dropFields.size > 0) {   
        scoreTable = scoreTable.drop(dropFields:_*)
      }
    
      var combinedResultTable = inputTable.join(scoreTable, Seq(idColumn), joinType = "left")
      val result = combinedResultTable.dropDuplicates(groupByColumn)
      lattice.output = result::Nil
    }
}
