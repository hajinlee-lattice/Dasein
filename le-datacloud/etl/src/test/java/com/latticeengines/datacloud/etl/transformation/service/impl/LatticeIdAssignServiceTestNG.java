package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.LatticeIdRefreshConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class LatticeIdAssignServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(LatticeIdAssignServiceTestNG.class);

    @Autowired
    AccountMasterSeed source;

    GeneralSource amId = new GeneralSource("AccountMasterId");

    String targetSourceName = "AccountMasterSeed";

    ObjectMapper om = new ObjectMapper();

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
            baseSources.add(amId.getSourceName());
            baseSources.add(source.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer("latticeIdAssignTransformer");
            step1.setTargetSource(targetSourceName);
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
        config.setStrategy("AccountMasterSeedRebuild");
        return om.writeValueAsString(config);
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
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
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
            { 10_000_000_001L, "dom1.com", "DUNS1" }, //
            { 10_000_000_002L, "dom1.com", "DUNS2" }, //
            { 20_000_000_003L, null, "DUNS2" }, //
            { 20_000_000_004L, null, "DUNS3" }, //
            { 30_000_000_001L, "dom2.com", "DUNS4" }, //
            { 30_000_000_002L, "dom3.com", "DUNS4" }, //
            { 40_000_000_001L, "dom3.com", null }, //
    };

    private void prepareAMId() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeAccountId", Long.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
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
        int rowNum = 0;
        Set<Long> ids = new HashSet<Long>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long id = (Long) record.get("LatticeAccountId");
            Assert.assertNotNull(id);
            Assert.assertFalse(ids.contains(id));
            ids.add(id);
            Object duns = record.get("DUNS");
            if (duns instanceof Utf8) {
                duns = duns.toString();
            }
            Object domain = record.get("Domain");
            if (domain instanceof Utf8) {
                domain = domain.toString();
            }
            log.info(String.format("LatticeAccountId = %d, Domain = %s, DUNS = %s", id, domain, duns));
            boolean flag = false;
            for (Object[] data : expectedData) {
                if (id.longValue() == ((Long) data[0]).longValue() //
                        && ((domain == null && data[1] == null) || (domain != null && domain.equals(data[1]))) //
                        && ((duns == null && data[2] == null) || (duns != null && duns.equals(data[2])))) {
                    flag = true;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 4);
    }

}
