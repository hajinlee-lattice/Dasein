package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AMAttrEnricher;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceProfiler;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMAttrEnrichConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceProfileDeploymentTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(SourceProfileDeploymentTestNG.class);

    private static final long RAND_SEED = 0L;

    private GeneralSource source = new GeneralSource("Profile");

    @Autowired
    private AccountMaster am;

    private static final String customerTableName = "CustomerTable";
    GeneralSource customerTable = new GeneralSource(customerTableName);

    @Autowired
    private SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    private ObjectMapper om = new ObjectMapper();

    @Test(groups = "deployment")
    public void testTransformation() {
        prepareAM();
        prepareCustomer();
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

            TransformationStepConfig step0 = new TransformationStepConfig();
            SourceTable sourceTable = new SourceTable(customerTable.getSourceName(),
                    CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            List<String> baseSources = new ArrayList<>();
            baseSources.add(customerTable.getSourceName());
            baseSources.add(am.getSourceName());
            step0.setBaseSources(baseSources);
            Map<String, SourceTable> baseTables = new HashMap<>();
            baseTables.put(customerTable.getSourceName(), sourceTable);
            step0.setBaseTables(baseTables);
            step0.setTransformer(AMAttrEnricher.TRANSFORMER_NAME);
            step0.setConfiguration(getCustomerUniverseConfig());
            step0.setTargetSource("Enriched");

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<>();
            inputSteps.addAll(Collections.singletonList(0));
            step1.setInputSteps(inputSteps);
            step1.setTransformer(SourceProfiler.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getProfileConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step0);
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);
            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            configuration.setKeepTemp(true);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getCustomerUniverseConfig() throws JsonProcessingException {
        AMAttrEnrichConfig conf = new AMAttrEnrichConfig();
        conf.setAmLatticeId("LatticeID");
        conf.setInputLatticeId("LatticeAccountId");
        return om.writeValueAsString(conf);
    }

    private String getProfileConfig() throws JsonProcessingException {
        ProfileConfig conf = new ProfileConfig();
        conf.setNumBucketEqualSized(false);
        conf.setBucketNum(4);
        conf.setMinBucketSize(2);
        conf.setRandSeed(RAND_SEED);
        return om.writeValueAsString(conf);
    }

    private void prepareCustomer() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeAccountId", Long.class)); // Retained
        columns.add(Pair.of("Customer1", Integer.class)); // Interval
        columns.add(Pair.of("Customer2", Long.class)); // Interval
        columns.add(Pair.of("Customer3", Float.class)); // Interval
        columns.add(Pair.of("Customer4", Double.class)); // Interval
        columns.add(Pair.of("Customer5", Boolean.class)); // Boolean
        columns.add(Pair.of("Customer6", Double.class)); // Interval (use distinct value as interval boundary)

        Object[][] data = new Object[][] { //
                { 1L, 0, null, 10F, null, true, 1.01 }, //
                { 2L, null, 10L, null, 100D, false, 1.01 }, //
                { 3L, 10, null, 100F, 100D, null, 1.01 }, //
                { 4L, null, 100L, 100F, 1000D, true, 1.01 }, //
                { 5L, 100, 100L, 1000F, 1000D, false, 2.02 }, //
                { 6L, 100, 1000L, 1000F, 10000D, null, 2.02 }, //
                { 7L, 1000, 1000L, 10000F, 10000D, true, 2.02 }, //
                { 8L, 1000, 10000L, 10000F, null, false, null }, //
                { 9L, 10000, 10000L, null, 0D, null, null }, //
                { 10L, 10000, null, 0F, 100D, true, null }, //
                { 11L, null, 0L, 100F, 10D, false, null }, //
                { 12L, 0, 100L, 10F, 0D, null, null }, //
                { 13L, 100, 10L, 0F, null, true, null }, //
                { 14L, 10, 0L, null, 10D, false, null }, //
        };
        uploadAndRegisterTableSource(columns, data, customerTable.getSourceName());
    }

    private void prepareAM() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeID", Long.class)); // Retained
        columns.add(Pair.of("AlexaAUPageViews", Integer.class)); // Interval
        columns.add(Pair.of("AlexaAURank", Long.class)); // Interval
        columns.add(Pair.of("AlexaAUUsers", Float.class)); // Interval
        columns.add(Pair.of("AlexaCAPageViews", Double.class)); // Interval
        columns.add(Pair.of("AlexaCategories", String.class)); // Retained
        columns.add(Pair.of("AlexaCARank", Integer.class)); // Retained (Numeric without any value)
        columns.add(Pair.of("AlexaCAUsers", Boolean.class)); // Boolean
        columns.add(Pair.of("AlexaGBPageViews", Boolean.class)); // Boolean
        columns.add(Pair.of("AlexaDomains", Boolean.class)); // Discarded
        columns.add(Pair.of("HGData_SupplierTechIndicators", String.class)); // Boolean (need to decode)
        columns.add(Pair.of("BuiltWith_TechIndicators", String.class)); // Boolean (need to decode)

        Object[][] data = new Object[][] { //
                { 1L, 79, 79L, 79F, 79D, "TestRetained", null, true, true, true, null, null, }, //
                { 2L, 15, 15L, 14.89482594F, 14.89482594D, "TestRetained", null, true, true, true, null, null, }, //
                { 3L, -5, -5L, -5F, -5D, "TestRetained", null, true, true, true, null, null, }, //
                { 4L, 2, 2L, 2F, 2D, "TestRetained", null, true, true, true, null, null, }, //
                { 5L, -2, -2L, -2.40582905F, -2.40582905D, "TestRetained", null, true, true, true, null, null, }, //
                { 6L, 9162, 9162L, 9162F, 9162D, "TestRetained", null, true, true, true, null, null, }, //
                { 7L, 0, 0L, 0F, 0D, "TestRetained", null, true, true, true, null, null, }, //
                { 8L, 1, 1L, 1F, 1D, "TestRetained", null, true, true, true, null, null, }, //
                { 9L, 2, 2L, 2F, 2D, "TestRetained", null, true, true, true, null, null, }, //
                { 10L, 2, 2L, 2.12F, 2.12D, "TestRetained", null, true, true, true, null, null, }, //
                { 11L, 44, 44L, 44F, 44D, "TestRetained", null, true, true, true, null, null, }, //
                { 12L, 100002, 100002L, 100002F, 100002D, "TestRetained", null, true, true, true, null, null, }, //
                { 13L, 737, 737L, 737F, 737D, "TestRetained", null, true, true, true, null, null, }, //
                { 14L, 858, 858L, 858F, 858D, "TestRetained", null, true, true, true, null, null, }, //
                
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
            if (!StringUtils.contains(record.toString(), "TechIndicator")) {
                log.info(record);
            }
        }
    }

}
