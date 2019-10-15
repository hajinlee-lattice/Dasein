package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CombineInputTableWithScoreJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CombineInputTableWithScoreIdTestNG extends SparkJobFunctionalTestNGBase {

    private static final Double LIFT_1 = 3.4;
    private static final Double LIFT_2 = 2.4;
    private static final Double LIFT_3 = 1.2;
    private static final Double LIFT_4 = 0.4;

    private static final int NUM_LEADS_BUCKET_1 = 28588;
    private static final int NUM_LEADS_BUCKET_2 = 14534;
    private static final int NUM_LEADS_BUCKET_3 = 25206;
    private static final int NUM_LEADS_BUCKET_4 = 25565;

    private CombineInputTableWithScoreJobConfig getJobConfiguration() {
        CombineInputTableWithScoreJobConfig config = new CombineInputTableWithScoreJobConfig();
        config.bucketMetadata = generateDefaultBucketMetadata();
        config.idColumn = InterfaceName.InternalId.name();
        return config;
    }

    private CombineInputTableWithScoreJobConfig getParametersWithPMMLModelType() {
        CombineInputTableWithScoreJobConfig config = new CombineInputTableWithScoreJobConfig();
        config.bucketMetadata = generateDefaultBucketMetadata();
        config.idColumn = InterfaceName.InternalId.name();
        config.modelType = ModelType.PMML.getModelType();
        return config;
    }

    private List<BucketMetadata> generateDefaultBucketMetadata() {
        List<BucketMetadata> bucketMetadataList = new ArrayList<BucketMetadata>();
        BucketMetadata BUCKET_METADATA_A = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_B = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_C = new BucketMetadata();
        BucketMetadata BUCKET_METADATA_D = new BucketMetadata();
        bucketMetadataList.add(BUCKET_METADATA_A);
        bucketMetadataList.add(BUCKET_METADATA_B);
        bucketMetadataList.add(BUCKET_METADATA_C);
        bucketMetadataList.add(BUCKET_METADATA_D);
        BUCKET_METADATA_A.setBucket(BucketName.A_PLUS);
        BUCKET_METADATA_A.setNumLeads(NUM_LEADS_BUCKET_1);
        BUCKET_METADATA_A.setLeftBoundScore(99);
        BUCKET_METADATA_A.setRightBoundScore(91);
        BUCKET_METADATA_A.setLift(LIFT_1);
        BUCKET_METADATA_B.setBucket(BucketName.B);
        BUCKET_METADATA_B.setNumLeads(NUM_LEADS_BUCKET_2);
        BUCKET_METADATA_B.setLeftBoundScore(90);
        BUCKET_METADATA_B.setRightBoundScore(85);
        BUCKET_METADATA_B.setLift(LIFT_2);
        BUCKET_METADATA_C.setBucket(BucketName.C);
        BUCKET_METADATA_C.setNumLeads(NUM_LEADS_BUCKET_3);
        BUCKET_METADATA_C.setLeftBoundScore(84);
        BUCKET_METADATA_C.setRightBoundScore(50);
        BUCKET_METADATA_C.setLift(LIFT_3);
        BUCKET_METADATA_D.setBucket(BucketName.D);
        BUCKET_METADATA_D.setNumLeads(NUM_LEADS_BUCKET_4);
        BUCKET_METADATA_D.setLeftBoundScore(49);
        BUCKET_METADATA_D.setRightBoundScore(5);
        BUCKET_METADATA_D.setLift(LIFT_4);
        return bucketMetadataList;
    }

    @Override
    protected String getJobName() {
        return "combineInputTableWithScore";
    }

    @Override
    protected String getScenarioName() {
        return "idBased";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable", "ScoreResult");
    }

    @Test(groups = "functional")
    public void testScoresAreCombined() throws Exception {

        SparkJobResult result = runSparkJob(CombineInputTableWithScoreJob.class, getJobConfiguration());
        verify(result, Collections.singletonList(this::verifyScoresAreCombined));
    }

    private Boolean verifyScoresAreCombined(HdfsDataUnit tgt) {

        try {
            List<GenericRecord> inputRecords = AvroUtils.readFromLocalFile(ClassLoader
                    .getSystemResource(String.format("%s/%s/%s/part-m-00000.avro", //
                            getJobName(), getScenarioName(), "InputTable")) //
                    .getPath());

            int outputRecords = 0;
            boolean foundAplusScore = false;
            List<GenericRecord> records = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                records.add(record);
            });
            for (GenericRecord record : records) {
                assertNotNull(record.get(InterfaceName.InternalId.name()));
                assertNotNull(record.get(ScoreResultField.Percentile.displayName));
                assertNotNull(record.get(ScoreResultField.Rating.displayName));
                if (record.get(ScoreResultField.Rating.displayName).toString().equals(BucketName.A_PLUS.toValue())) {
                    foundAplusScore = true;
                }
                outputRecords++;
            }
            assertEquals(outputRecords, inputRecords.size() - 1);
            assertTrue(foundAplusScore, "Should have found one A+ bucket.");
            return true;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "testScoresAreCombined")
    public void testRatingsAreNotGeneratedForPMMLModel() throws IOException {

        SparkJobResult result = runSparkJob(CombineInputTableWithScoreJob.class, getParametersWithPMMLModelType());
        verify(result, Collections.singletonList(this::verifyRatingsAreNotGeneratedForPMMLModel));
    }

    private Boolean verifyRatingsAreNotGeneratedForPMMLModel(HdfsDataUnit tgt) {
        try {
            List<GenericRecord> inputRecords = AvroUtils.readFromLocalFile(ClassLoader
                    .getSystemResource(String.format("%s/%s/%s/part-m-00000.avro", //
                            getJobName(), getScenarioName(), "InputTable")) //
                    .getPath());

            List<GenericRecord> outputRecords = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                outputRecords.add(record);
            });
            assertEquals(outputRecords.size(), inputRecords.size() - 1);
            for (GenericRecord record : outputRecords) {
                assertNotNull(record.get(InterfaceName.InternalId.name()));
                assertNotNull(record.get(ScoreResultField.Percentile.displayName));
                assertNull(record.get(ScoreResultField.Rating.displayName));
            }
            return true;

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
