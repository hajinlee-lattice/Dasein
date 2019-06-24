package com.latticeengines.datacloud.etl.transformation.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.HQ_DUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.HQ_DUNS_DOMAIN;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.transformation.AMStatsHQDuns;
import com.latticeengines.datacloud.dataflow.transformation.AMStatsReport;
import com.latticeengines.datacloud.dataflow.transformation.ExtractCube;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccountMasterStatisticsTestNG extends AccountMasterBucketTestNG {

    private static final Logger log = LoggerFactory.getLogger(AccountMasterStatisticsTestNG.class);
    private static final List<String> dimensions = Arrays.asList( //
            "LDC_Country", //
            "LDC_PrimaryIndustry", //
            "LE_REVENUE_RANGE", //
            "LE_EMPLOYEE_RANGE" //
    );

    private static final String DATA_CLOUD_VERSION = "2.0.6";

    @Override
    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() throws Exception {
        uploadBaseSourceFile(accountMaster.getSourceName(), "AMBucketTest_AM", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // more about correctness is tested in the dataflow functional tests
        log.info("Start to verify records one by one ...");
        long count = 0;
        long zeroCountAttrs = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            count++;
            record.getSchema().getFields().forEach(field -> Assert.assertFalse(dimensions.contains(field.name()),
                    "Found a dimension attr in filtered cube: " + field.name()));
            if ((long) record.get(STATS_ATTR_COUNT) == 0L) {
                zeroCountAttrs++;
            }
        }
        logger.info(count + " records in total, " + zeroCountAttrs + " has zero not null count");
        // TODO: change to compare with rows in profile result
        Assert.assertEquals(count, 28574);
        Assert.assertTrue(zeroCountAttrs > 0, "Should have some attributes with zero count.");
    }

    @Override
    protected String getTargetSourceName() {
        return "AccountMasterEnrichmentStats";
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AccountMasterStatistics");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig profile = profile();
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig hqduns = hqduns();
            TransformationStepConfig calcStats = calcStats();
            TransformationStepConfig report = report();
            TransformationStepConfig extractTopCube = extractTopCube();
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    profile, //
                    bucket, //
                    hqduns, //
                    calcStats, //
                    report, //
                    extractTopCube);
            // -----------
            steps.get(steps.size() - 1).setTargetSource(getTargetSourceName());
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected TransformationStepConfig profile() {
        TransformationStepConfig step = super.profile();
        step.setTargetSource("AccountMasterProfile");
        return step;
    }

    @Override
    protected ProfileConfig constructProfileConfig() {
        ProfileConfig conf = new ProfileConfig();
        conf.setStage(DataCloudConstants.PROFILE_STAGE_ENRICH);
        conf.setDataCloudVersion(DATA_CLOUD_VERSION);
        conf.setCatAttrsNotEnc(dimensions.toArray(new String[dimensions.size()]));
        return conf;
    }

    @Override
    protected TransformationStepConfig bucket() {
        TransformationStepConfig step = super.bucket();
        step.setInputSteps(Collections.singletonList(0));
        return step;
    }

    private TransformationStepConfig hqduns() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Collections.singletonList("AccountMaster"));
        step.setInputSteps(Collections.singletonList(1));
        step.setTransformer(AMStatsHQDuns.TRANSFORMER_NAME);
        step.setConfiguration("{}");
        return step;
    }

    @Override
    protected TransformationStepConfig calcStats() {
        TransformationStepConfig step = super.calcStats();
        step.setInputSteps(Arrays.asList(0, 2));
        CalculateStatsConfig config = new CalculateStatsConfig();
        Map<String, List<String>> dims = new HashMap<>();
        dimensions.forEach(d -> dims.put(d, null));
        config.setDimensionTree(dims);
        config.setDedupFields(Arrays.asList(HQ_DUNS, HQ_DUNS_DOMAIN));
        String confStr = setDataFlowEngine(JsonUtils.serialize(config), "TEZ");
        step.setConfiguration(confStr);
        step.setTargetSource("AccountMasterStats");
        return step;
    }

    private TransformationStepConfig report() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(3));
        step.setTransformer(AMStatsReport.TRANSFORMER_NAME);
        step.setConfiguration("{}");
        step.setTargetSource("AccountMasterReport");
        return step;
    }

    private TransformationStepConfig extractTopCube() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Arrays.asList(0, 3));
        step.setTransformer(ExtractCube.TRANSFORMER_NAME);
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setPartitions(1);
        String confStr = setDataFlowEngine("{}", engineConf);
        step.setConfiguration(confStr);
        step.setTargetSource("AccountMasterEnrichmentStats");
        return step;
    }

}
