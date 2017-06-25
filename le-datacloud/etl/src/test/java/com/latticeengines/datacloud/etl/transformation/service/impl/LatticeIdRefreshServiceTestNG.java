package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.LatticeIdRefreshConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class LatticeIdRefreshServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(LatticeIdRefreshServiceTestNG.class);

    GeneralSource source = new GeneralSource("AccountMasterId");

    GeneralSource amsInit = new GeneralSource("AccountMasterSeedInit");

    GeneralSource amsRefresh = new GeneralSource("AccountMasterSeedRefresh");

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterId";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareAMSeedInit();
        prepareAMSeedRefresh();
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
            configuration.setName("AccountMasterIdRefresh");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<>();
            baseSources.add(source.getSourceName());
            baseSources.add(amsInit.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer("latticeIdRefreshTransformer");
            String confParamStr1 = getTransformerConfigForInit();
            step1.setConfiguration(confParamStr1);

            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            baseSources = new ArrayList<>();
            baseSources.add(amsRefresh.getSourceName());
            step2.setBaseSources(baseSources);
            step2.setTransformer("latticeIdRefreshTransformer");
            step2.setTargetSource(targetSourceName);
            String confParamStr2 = getTransformerConfigForRefresh();
            step2.setConfiguration(confParamStr2);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getTransformerConfigForInit() throws JsonProcessingException {
        LatticeIdRefreshConfig config = new LatticeIdRefreshConfig();
        config.setStrategy("AccountMasterSeedRebuild");
        return om.writeValueAsString(config);
    }

    private String getTransformerConfigForRefresh() throws JsonProcessingException {
        LatticeIdRefreshConfig config = new LatticeIdRefreshConfig();
        config.setStrategy("AccountMasterSeedRebuild");
        config.setCurrentCount(16L);
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

    private Object[][] amsInitData = new Object[][] { //
            { "dom1.com", "DUNS1" }, //
            { "dom1.com", "DUNS2" }, //
            { "dom2.com", "DUNS1" }, //
            { "dom2.com", "DUNS2" }, //
            { null, "DUNS1" }, //
            { null, "DUNS3" }, //
            { "dom1.com", null }, //
            { "dom3.com", null }, //
            { "dom11.com", "DUNS11" }, //
            { "dom11.com", "DUNS22" }, //
            { "dom22.com", "DUNS11" }, //
            { "dom22.com", "DUNS22" }, //
            { null, "DUNS11" }, //
            { null, "DUNS33" }, //
            { "dom11.com", null }, //
            { "dom33.com", null }, //
            { "dom1111.com", "DUNS1111" }, //
            { "dom2222.com", "DUNS2222" }, //
            { "dom3333.com", "DUNS3333" }, //
    };

    private void prepareAMSeedInit() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(amsInit.getSourceName(), baseSourceVersion, columns, amsInitData);
    }
    
    private Object[][] amsRefreshData = new Object[][] { //
            { "dom1.com", "DUNS1" }, //
            { "dom1.com", "DUNS2" }, //
            { "dom2.com", "DUNS1" }, //
            { "dom2.com", "DUNS2" }, //
            { null, "DUNS1" }, //
            { null, "DUNS3" }, //
            { "dom1.com", null }, //
            { "dom3.com", null }, //
            { "dom111.com", "DUNS111" }, //
            { "dom111.com", "DUNS222" }, //
            { "dom222.com", "DUNS111" }, //
            { "dom222.com", "DUNS222" }, //
            { null, "DUNS111" }, //
            { null, "DUNS333" }, //
            { "dom111.com", null }, //
            { "dom333.com", null }, //
            { "dom1111.com", null }, //
            { null, "DUNS1111" }, //
            { "dom2222.com", null }, //
            { null, "DUNS3333" }, //
    };

    private void prepareAMSeedRefresh() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(amsRefresh.getSourceName(), baseSourceVersion, columns, amsRefreshData);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Object[][] expectedData = new Object[][] { //
                { "dom1.com", "DUNS1", "ACTIVE" }, //
                { "dom1.com", "DUNS2", "ACTIVE" }, //
                { "dom2.com", "DUNS1", "ACTIVE" }, //
                { "dom2.com", "DUNS2", "ACTIVE" }, //
                { null, "DUNS1", "ACTIVE" }, //
                { null, "DUNS3", "ACTIVE" }, //
                { "dom1.com", null, "ACTIVE" }, //
                { "dom3.com", null, "ACTIVE" }, //
                { "dom111.com", "DUNS111", "ACTIVE" }, //
                { "dom111.com", "DUNS222", "ACTIVE" }, //
                { "dom222.com", "DUNS111", "ACTIVE" }, //
                { "dom222.com", "DUNS222", "ACTIVE" }, //
                { null, "DUNS111", "ACTIVE" }, //
                { null, "DUNS333", "ACTIVE" }, //
                { "dom111.com", null, "ACTIVE" }, //
                { "dom333.com", null, "ACTIVE" }, //
                { "dom11.com", "DUNS11", "OBSOLETE" }, //
                { "dom11.com", "DUNS22", "OBSOLETE" }, //
                { "dom22.com", "DUNS11", "OBSOLETE" }, //
                { "dom22.com", "DUNS22", "OBSOLETE" }, //
                { null, "DUNS11", "OBSOLETE" }, //
                { null, "DUNS33", "OBSOLETE" }, //
                { "dom1111.com", "DUNS1111", "UPDATED" }, //
                { "dom2222.com", "DUNS2222", "UPDATED" }, //
                { "dom3333.com", "DUNS3333", "UPDATED" }, //
                { "dom1111.com", null, "ACTIVE" }, //
                { null, "DUNS1111", "ACTIVE" }, //
                { "dom2222.com", null, "ACTIVE" }, //
                { null, "DUNS3333", "ACTIVE" }, //
        };
        int rowNum = 0;
        Set<Long> ids = new HashSet<Long>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long id = (Long) record.get("LatticeAccountId");
            Long redirectFromId = (Long) record.get("RedirectFromId");
            Assert.assertFalse(ids.contains(redirectFromId));
            ids.add(redirectFromId);
            Object duns = record.get("DUNS");
            if (duns instanceof Utf8) {
                duns = duns.toString();
            }
            Object domain = record.get("Domain");
            if (domain instanceof Utf8) {
                domain = domain.toString();
            }
            Object status = record.get("Status");
            if (status instanceof Utf8) {
                status = status.toString();
            }
            log.info(String.format("LatticeAccountId = %d, DUNS = %s, Domain = %s, Status = %s, RedirectedFromId = %d",
                    id, duns, domain, status, redirectFromId));
            boolean flag = false;
            for (Object[] data : expectedData) {
                if ((domain == null && data[0] == null) || (domain != null && domain.equals(data[0]))
                        || (duns == null && data[1] == null) || (duns != null && duns.equals(data[1]))
                        || (status == null && data[2] == null) || (status != null && status.equals(data[2]))) {
                    flag = true;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(31L, rowNum);
    }

}
