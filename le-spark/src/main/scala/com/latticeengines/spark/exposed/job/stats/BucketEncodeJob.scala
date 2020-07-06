package com.latticeengines.spark.exposed.job.stats

import com.latticeengines.common.exposed.util.BitCodecUtils
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.DCEncodedAttr
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook
import com.latticeengines.domain.exposed.spark.stats.BucketEncodeConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import com.latticeengines.spark.util.{BitEncodeUtils, BucketEncodeUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

class BucketEncodeJob extends AbstractSparkJob[BucketEncodeConfig] {

    private val ATTR_ATTRNAME = DataCloudConstants.PROFILE_ATTR_ATTRNAME

    override def runJob(spark: SparkSession, lattice: LatticeContext[BucketEncodeConfig]): Unit = {
        val inputData: DataFrame = lattice.input.head
        val profileData: DataFrame = lattice.input(1)
        val config: BucketEncodeConfig = lattice.config

        val renamedAttrs: Map[String, String] = if (config.getRenameFields == null) Map() else config.getRenameFields.asScala.toMap
        val renamed = renamedAttrs.foldLeft(inputData)((prev, t) => prev.withColumnRenamed(t._1, t._2))

        val profileAttrs: Seq[String] = profileData.select(ATTR_ATTRNAME).collect().map(r => r.getString(0))
        val expanded = expandEncodedByAM(renamed, profileAttrs, config)
        val outputSchema = getOutputSchema(expanded, config)
        val encoded = encode(spark, expanded, outputSchema, config)

        lattice.output = encoded :: Nil
    }

    // when running bucket encode on AM, there are big string attributes encoded by AM
    private def expandEncodedByAM(input: DataFrame, cols: Seq[String], config: BucketEncodeConfig): DataFrame = {
        val codeBookLookup: Map[String, String] = //
            if (config.getCodeBookLookup == null) Map() else config.getCodeBookLookup.asScala.toMap
        val codeBookMap: Map[String, BitCodeBook] = //
            if (config.getCodeBookMap == null) Map() else config.getCodeBookMap.asScala.toMap
        BitEncodeUtils.decode(input, cols, codeBookLookup, codeBookMap)
    }

    private def getOutputSchema(input: DataFrame, config: BucketEncodeConfig): Seq[StructField] = {
        val renamedAttrs: Map[String, String] =
            if (config.getRenameFields == null) Map() else config.getRenameFields.asScala.toMap
        val retainAttrsInConfig: Seq[String] = (if (config.retainAttrs == null) Seq() else config.retainAttrs.asScala)
          .map(a => renamedAttrs.getOrElse(a, a))
        val retainAttrs: Set[String] = input.columns.intersect(retainAttrsInConfig).toSet
        val retainFields: Seq[StructField] = input.schema.fields.filter(f => retainAttrs.contains(f.name))
        val encAttrs: Seq[DCEncodedAttr] = if(config.getEncAttrs == null) Seq() else config.getEncAttrs.asScala
        val encFields: Seq[StructField] = encAttrs.map(ea => StructField(ea.getEncAttr, LongType, nullable = true))
        retainFields ++ encFields
    }

    private def encode(spark: SparkSession, input: DataFrame, outputSchema: Seq[StructField], config: BucketEncodeConfig): DataFrame = {
        val inputPos: Map[String, Int] = input.columns.zipWithIndex.toMap
        val bEAttrs: Broadcast[Map[String, DCEncodedAttr]] = spark.sparkContext.broadcast(
            if (config.getEncAttrs == null) {
                Map()
            } else {
                config.getEncAttrs.asScala.map(ea => (ea.getEncAttr, ea)).toMap
            })
        input.map(r => {
            val eAttrs = bEAttrs.value
            val values: Seq[Any] = outputSchema map (f => {
                val attr = f.name
                if (inputPos.contains(attr)) {
                    r.get(inputPos(attr))
                } else if (eAttrs.contains(attr)) {
                    val eAttr = eAttrs(attr)
                    eAttr.getBktAttrs.asScala.foldLeft(0L)((prev, bAttr) => {
                        val nAttr = bAttr.getNominalAttr
                        if (inputPos.contains(nAttr)) {
                            val obj = r.get(inputPos(nAttr))
                            val bktIdx = BucketEncodeUtils.bucket(obj, bAttr.getBucketAlgo)
                            BitCodecUtils.setBits(prev, bAttr.getLowestBit, bAttr.getNumBits, bktIdx)
                        } else {
                            prev
                        }
                    })
                } else {
                    // an attr required by output but not exist in input
                    null
                }
            })
            Row.fromSeq(values)
        })(RowEncoder(StructType(outputSchema)))
    }

}
