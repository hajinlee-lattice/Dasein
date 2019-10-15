package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CombineInputTableWithScoreJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CombineInputTableWithScoreMultiModelTestNG extends SparkJobFunctionalTestNGBase {

    private CombineInputTableWithScoreJobConfig getJobConfiguration() {

        CombineInputTableWithScoreJobConfig config = new CombineInputTableWithScoreJobConfig();
        config.idColumn = InterfaceName.__Composite_Key__.name();
        config.modelIdField = ScoreResultField.ModelId.displayName;
        config.bucketMetadataMap = ImmutableMap.of( //
                "ms__6e6f1ad0-8ca5-4102-8477-0b9c79cca206-ai_6rvlw", getDefaultBucketMetadata(), //
                "ms__c0b0a2f0-8fd4-4817-aee6-fc47c8e6745b-ai_qr7cx", getDefaultBucketMetadata(), //
                "ms__308f62f0-addb-4dae-8a81-7ad2c55d5618-ai_c3lyq", getDefaultBucketMetadata(), //
                "ms__05e1fdff-69a4-4337-baee-c66cb315ee0a-ai_1mgui", getDefaultBucketMetadata() //

        );
        return config;
    }

    @Override
    protected String getJobName() {
        return "combineInputTableWithScore";
    }

    @Override
    protected String getScenarioName() {
        return "multiModel";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable", "ScoreResult");
    }

    @Test(groups = "functional")
    public void execute() {
        SparkJobResult result = runSparkJob(CombineInputTableWithScoreJob.class, getJobConfiguration());
        verify(result, Collections.singletonList(this::verifyScoresAreCombined));

    }

    private Boolean verifyScoresAreCombined(HdfsDataUnit tgt) {
        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });
        for (GenericRecord record : outputRecords) {
            System.out.println(record);
            assertNotNull(record.get(ScoreResultField.Rating.displayName));
        }
        return true;
    }

    private List<BucketMetadata> getDefaultBucketMetadata() {
        return BucketMetadataUtils.getDefaultMetadata();
    }

}
