package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.Profile;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceProfileTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(SourceProfileTestNG.class);

    GeneralSource source = new GeneralSource("AMProfile");

    @Autowired
    AccountMaster am;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline2")
    public void testTransformation() {
        prepareAM();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("SourceProfiling");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(am.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(Profile.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getProfileConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getProfileConfig() throws JsonProcessingException {
        ProfileConfig conf = new ProfileConfig();
        conf.setNumBucketEqualSized(false);
        conf.setBucketNum(4);
        conf.setMinBucketSize(2);
        return om.writeValueAsString(conf);
    }

    private void prepareAM() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("AlexaAUPageViews", Integer.class)); // Interval
        columns.add(Pair.of("AlexaAURank", Long.class)); // Interval
        columns.add(Pair.of("AlexaAUUsers", Float.class)); // Interval
        columns.add(Pair.of("AlexaCAPageViews", Double.class)); // Interval
        columns.add(Pair.of("AlexaCategories", String.class)); // Retained
        columns.add(Pair.of("AlexaCARank", Integer.class)); // Retained
        columns.add(Pair.of("AlexaCAUsers", Boolean.class)); // Boolean
        columns.add(Pair.of("AlexaDescription", Boolean.class)); // Boolean
        columns.add(Pair.of("AlexaDomains", Boolean.class)); // Boolean
        columns.add(Pair.of("HGData_SupplierTechIndicators", String.class)); // Encoded
        columns.add(Pair.of("BuiltWith_TechIndicators", String.class)); // Encoded

        Object[][] data = new Object[][] { //
                { 79, 79L, 79F, 79D, "TestRetained", null, true, true, true, null, null }, //
                { 15, 15L, 14.89482594F, 14.89482594D, "TestRetained", null, true, true, true, null, null }, //
                { -5, -5L, -5F, -5D, "TestRetained", null, true, true, true, null, null }, //
                { 2, 2L, 2F, 2D, "TestRetained", null, true, true, true, null, null }, //
                { -2, -2L, -2.40582905F, -2.40582905D, "TestRetained", null, true, true, true, null, null }, //
                { 9162, 9162L, 9162F, 9162D, "TestRetained", null, true, true, true, null, null }, //
                { 0, 0L, 0F, 0D, "TestRetained", null, true, true, true, null, null }, //
                { 1, 1L, 1F, 1D, "TestRetained", null, true, true, true, null, null }, //
                { 2, 2L, 2F, 2D, "TestRetained", null, true, true, true, null, null }, //
                { 2, 2L, 2.12F, 2.12D, "TestRetained", null, true, true, true, null, null }, //
                { 44, 44L, 44F, 44D, "TestRetained", null, true, true, true, null, null }, //
                { 100002, 100002L, 100002F, 100002D, "TestRetained", null, true, true, true, null, null }, //
                { 737, 737L, 737F, 737D, "TestRetained", null, true, true, true, null, null }, //
                { 858, 858L, 858F, 858D, "TestRetained", null, true, true, true, null, null }, //
                
        };

        uploadBaseSourceData(am.getSourceName(), baseSourceVersion, columns, data);
        try {
            extractSchema(am, baseSourceVersion,
                    hdfsPathBuilder.constructSnapshotDir(am.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s", am.getSourceName(),
                    baseSourceVersion));
        }
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record);
        }
    }

}
