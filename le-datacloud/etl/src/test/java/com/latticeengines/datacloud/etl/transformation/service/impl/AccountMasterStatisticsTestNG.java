package com.latticeengines.datacloud.etl.transformation.service.impl;


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
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.transformation.AMStatsHQDuns;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccountMasterStatisticsTestNG extends AccountMasterBucketTestNG {

    private static final Logger log = LoggerFactory.getLogger(AccountMasterStatisticsTestNG.class);

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
        // correctness is tested in the dataflow functional test
        log.info("Start to verify records one by one.");
//        while (records.hasNext()) {
//            GenericRecord record = records.next();
//            System.out.println(record);
//        }
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
            TransformationStepConfig profile = profile(DataCloudConstants.PROFILE_STAGE_ENRICH);
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig hqduns = hqduns();
            TransformationStepConfig calcStats = calcStats();
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    profile, //
                    bucket, //
                    hqduns, //
                    calcStats
            );
            // -----------
            steps.get(steps.size() - 1).setTargetSource(getTargetSourceName());
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected TransformationStepConfig profile(String stage) {
        TransformationStepConfig step = super.profile(stage);
        step.setTargetSource("AccountMasterProfile");
        return step;
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
        TransformerConfig conf = new TransformerConfig();
        String confStr = setDataFlowEngine(JsonUtils.serialize(conf), "TEZ");
        step.setConfiguration(confStr);
        return step;
    }

    @Override
    protected TransformationStepConfig calcStats() {
        TransformationStepConfig step = super.calcStats();
        step.setInputSteps(Arrays.asList(0, 2));
        CalculateStatsConfig config = new CalculateStatsConfig();
        Map<String, List<String>> dims = new HashMap<>();
        dims.put("LDC_Country", null);
        dims.put("LDC_PrimaryIndustry", null);
        dims.put("LE_REVENUE_RANGE", null);
        dims.put("LE_EMPLOYEE_RANGE", null);
        config.setDimensionGraph(dims);
        config.setDedupFields(Arrays.asList(HQ_DUNS, HQ_DUNS_DOMAIN));
        String confStr = setDataFlowEngine(JsonUtils.serialize(config), "TEZ");
        step.setConfiguration(confStr);
        step.setTargetSource("AccountMasterStatistics");
        return step;
    }

}
