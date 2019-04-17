package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.LatticeIdAssignTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.LatticeIdRefreshConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class LatticeIdAssignServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(LatticeIdAssignServiceTestNG.class);

    @Autowired
    AccountMasterSeed source;

    GeneralSource amId = new GeneralSource("AccountMasterId");

    private static final String STRATEGY = "AccountMasterSeedRebuild";

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareAMSeed();
        prepareAMId();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }


    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("LatticeIdAssign");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(source.getSourceName());
            baseSources.add(amId.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(LatticeIdAssignTransformer.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
            String confParamStr1 = getTransformerConfig();
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

    private String getTransformerConfig() throws JsonProcessingException {
        LatticeIdRefreshConfig config = new LatticeIdRefreshConfig();
        config.setStrategy(STRATEGY);
        return JsonUtils.serialize(config);
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private Object[][] amsData = new Object[][] { //
            { "dom1.com", "DUNS1" }, //
            { null, "DUNS2" }, //
            { "dom3.com", "DUNS4" }, //
            { "dom3.com", null }, //
    };

    private void prepareAMSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(source.getSourceName(), baseSourceVersion, columns, amsData);
    }

    private Object[][] amIdData = new Object[][] { //
            { 10_000_000_001L, "dom1.com", "DUNS1", "ACTIVE" }, //
            { 10_000_000_002L, "dom1.com", "DUNS2", "ACTIVE" }, //
            { 20_000_000_003L, null, "DUNS2", "ACTIVE" }, //
            { 20_000_000_004L, null, "DUNS3", "ACTIVE" }, //
            { 30_000_000_001L, "dom2.com", "DUNS4", "ACTIVE" }, //
            { 30_000_000_002L, "dom3.com", "DUNS4", "ACTIVE" }, //
            { 40_000_000_001L, "dom3.com", null, "ACTIVE" }, //
    };

    private void prepareAMId() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeID", Long.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("Status", String.class));
        uploadBaseSourceData(amId.getSourceName(), baseSourceVersion, columns, amIdData);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Object[][] expectedData = new Object[][] { //
                { 10_000_000_001L, "dom1.com", "DUNS1" }, //
                { 20_000_000_003L, null, "DUNS2" }, //
                { 30_000_000_002L, "dom3.com", "DUNS4" }, //
                { 40_000_000_001L, "dom3.com", null }, //
        };
        Map<String, Long> expected = new HashMap<>();
        for (Object[] data : expectedData) {
            expected.put(String.valueOf(data[1]) + String.valueOf(data[2]), (Long) data[0]);
        }
        int rowNum = 0;
        Set<Long> ids = new HashSet<Long>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Long id = (Long) record.get("LatticeID");
            Assert.assertNotNull(id);
            Assert.assertFalse(ids.contains(id));
            ids.add(id);
            Assert.assertEquals(id,
                    expected.get(String.valueOf(record.get("Domain")) + String.valueOf(record.get("DUNS"))));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 4);
    }

}
