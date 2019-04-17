package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
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
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.LatticeIdRefreshTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.LatticeIdRefreshConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class LatticeIdRefreshServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(LatticeIdRefreshServiceTestNG.class);

    private GeneralSource source = new GeneralSource("AMID");
    private GeneralSource amsInit = new GeneralSource("AMSeedInit");
    private GeneralSource amsRefresh = new GeneralSource("AMSeedRefresh");
    private GeneralSource amsSecondRefresh = new GeneralSource("AMSeedSecondRefresh");

    private static final String STRATEGY = "AccountMasterSeedRebuild";

    private static final String AMID_VERSION_EMPTY = "2017-06-01_00-00-00_UTC";
    private static final String AMID_VERSION_INIT = "2017-07-01_00-00-00_UTC";
    private static final String AMID_VERSION_REFRESH = "2017-08-01_00-00-00_UTC";

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareAMIDEmpty();
        prepareAMSeedInit();
        prepareAMSeedRefresh();
        prepareSecondAMSeedRefresh();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(source, AMID_VERSION_REFRESH);
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
            step1.setTargetSource(source.getSourceName());
            step1.setTargetVersion(AMID_VERSION_INIT);
            step1.setTransformer(LatticeIdRefreshTransformer.TRANSFORMER_NAME);
            String confParamStr1 = getTransformerConfigForInit();
            step1.setConfiguration(confParamStr1);

            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            baseSources = new ArrayList<>();
            baseSources.add(amsRefresh.getSourceName());
            step2.setBaseSources(baseSources);
            step2.setTargetVersion(AMID_VERSION_REFRESH);
            step2.setTransformer(LatticeIdRefreshTransformer.TRANSFORMER_NAME);
            step2.setTargetSource(source.getSourceName());
            String confParamStr2 = getTransformerConfigForRefresh();
            step2.setConfiguration(confParamStr2);

            TransformationStepConfig step3 = new TransformationStepConfig();
            inputSteps = new ArrayList<>();
            inputSteps.add(1);
            step3.setInputSteps(inputSteps);
            baseSources = new ArrayList<>();
            baseSources.add(amsSecondRefresh.getSourceName());
            step3.setBaseSources(baseSources);
            step3.setTransformer(LatticeIdRefreshTransformer.TRANSFORMER_NAME);
            step3.setTargetSource(source.getSourceName());
            String confParamStr3 = getTransformerConfigForRefresh();
            step3.setConfiguration(confParamStr3);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            steps.add(step3);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getTransformerConfigForInit() throws JsonProcessingException {
        LatticeIdRefreshConfig config = new LatticeIdRefreshConfig();
        config.setStrategy(STRATEGY);
        config.setIdSrcIdx(0);
        config.setEntitySrcIdx(1);
        return JsonUtils.serialize(config);
    }

    private String getTransformerConfigForRefresh() throws JsonProcessingException {
        LatticeIdRefreshConfig config = new LatticeIdRefreshConfig();
        config.setStrategy(STRATEGY);
        config.setIdSrcIdx(0);
        config.setEntitySrcIdx(1);
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

    private void prepareAMIDEmpty() {
        Object[][] data = new Object[0][0];
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("LatticeID", Long.class));
        columns.add(Pair.of("RedirectFromId", Long.class));
        columns.add(Pair.of("Status", String.class));
        columns.add(Pair.of("LE_Last_Update_Date", Long.class));
        uploadBaseSourceData(source.getSourceName(), AMID_VERSION_EMPTY, columns, data);
    }

    private Object[][] amsInitData = new Object[][] { //
            { "dom1.com", "DUNS1" }, // Not to refresh
            { "dom1.com", "DUNS2" }, // Not to refresh
            { "dom2.com", "DUNS1" }, // Not to refresh
            { "dom2.com", "DUNS2" }, // Not to refresh
            { null, "DUNS1" }, // Not to refresh
            { null, "DUNS3" }, // Not to refresh
            { "dom1.com", null }, // Not to refresh
            { "dom3.com", null }, // Not to refresh
            { "dom11.com", "DUNS11" }, // To retire
            { "dom11.com", "DUNS22" }, // To retire
            { "dom22.com", "DUNS11" }, // To retire
            { "dom22.com", "DUNS22" }, // To retire
            { null, "DUNS11" }, // To retire
            { null, "DUNS33" }, // To retire
            { "dom11.com", null }, // To retire
            { "dom33.com", null }, // To retire
            { "dom1111.com", "DUNS1111" }, // To redirect
            { "dom2222.com", "DUNS2222" }, // To redirect
            { "dom3333.com", "DUNS3333" }, // To redirect
            { "dom11111.com", "DUNS11111" }, // To obsolete
    };

    private void prepareAMSeedInit() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(amsInit.getSourceName(), baseSourceVersion, columns, amsInitData);
    }
    
    private Object[][] amsRefreshData = new Object[][] { //
            { "dom1.com", "DUNS1" }, // Not refreshed compared to init version
            { "dom1.com", "DUNS2" }, // Not refreshed compared to init version
            { "dom2.com", "DUNS1" }, // Not refreshed compared to init version
            { "dom2.com", "DUNS2" }, // Not refreshed compared to init version
            { null, "DUNS1" }, // Not refreshed compared to init version
            { null, "DUNS3" }, // Not refreshed compared to init version
            { "dom1.com", null }, // Not refreshed compared to init version
            { "dom3.com", null }, // Not refreshed compared to init version
            { "dom111.com", "DUNS111" }, // New compared to init version
            { "dom111.com", "DUNS222" }, // New compared to init version
            { "dom222.com", "DUNS111" }, // New compared to init version
            { "dom222.com", "DUNS222" }, // New compared to init version
            { null, "DUNS111" }, // New compared to init version
            { null, "DUNS333" }, // New compared to init version
            { "dom111.com", null }, // New compared to init version
            { "dom333.com", null }, // New compared to init version
            { "dom1111.com", null }, // New compared to init version
            { null, "DUNS1111" }, // New compared to init version
            { "dom2222.com", null }, // New compared to init version
            { null, "DUNS3333" }, // New compared to init version
            { "dom11111.comNew", "DUNS11111" }, // New compared to init version
            { "dom11111.com", "DUNS11111New" }, // New compared to init version
    };

    private void prepareAMSeedRefresh() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(amsRefresh.getSourceName(), baseSourceVersion, columns, amsRefreshData);
    }

    private Object[][] amsSecondRefreshData = new Object[][] { //
            { "dom1.com", "DUNS1" }, // Not refreshed compared to refresh version
            { "dom1.com", "DUNS2" }, // Not refreshed compared to refresh version
            { "dom2.com", "DUNS1" }, // Not refreshed compared to refresh version
            { "dom2.com", "DUNS2" }, // Not refreshed compared to refresh version
            { null, "DUNS1" }, // Not refreshed compared to refresh version
            { null, "DUNS3" }, // Not refreshed compared to refresh version
            { "dom1.com", null }, // Not refreshed compared to refresh version
            { "dom3.com", null }, // Not refreshed compared to refresh version
            { "dom111.com", "DUNS111" }, // Not refreshed compared to refresh version
            { "dom111.com", "DUNS222" }, // Not refreshed compared to refresh version
            { "dom222.com", "DUNS111" }, // Not refreshed compared to refresh version
            { "dom222.com", "DUNS222" }, // Not refreshed compared to refresh version
            { null, "DUNS111" }, // Not refreshed compared to refresh version
            { null, "DUNS333" }, // Not refreshed compared to refresh version
            { "dom111.com", null }, // Not refreshed compared to refresh version
            { "dom333.com", null }, // Not refreshed compared to refresh version
            { "dom1111.com", null }, // Not refreshed compared to refresh version
            { null, "DUNS1111" }, // Not refreshed compared to refresh version
            { "dom2222.com", null }, // Not refreshed compared to refresh version
            { null, "DUNS3333" }, // Not refreshed compared to refresh version
            { "dom11111.comNew", "DUNS11111" }, // Not refreshed compared to refresh version
            { "dom11111.com", "DUNS11111New" }, // Not refreshed compared to refresh version
            { "dom33.com", null }, // OBSOLETE -> ACTIVE compared to refresh version
            { "dom1111.com", "DUNS1111" }, // UPDATED -> ACTIVE compared to refresh version
    };

    private void prepareSecondAMSeedRefresh() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(amsSecondRefresh.getSourceName(), baseSourceVersion, columns, amsSecondRefreshData);
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info("Start to verify " + source + " @" + version);
        switch (version) {
        case AMID_VERSION_REFRESH:
            verifyAMID2ndRefresh(records);
            break;
        default:
            break;
        }
    }

    private void verifyAMID2ndRefresh(Iterator<GenericRecord> records) {
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
                { "dom11.com", null, "OBSOLETE" }, //
                { "dom33.com", null, "OBSOLETE" }, //
                { "dom1111.com", "DUNS1111", "UPDATED" }, //
                { "dom2222.com", "DUNS2222", "UPDATED" }, //
                { "dom3333.com", "DUNS3333", "UPDATED" }, //
                { "dom1111.com", null, "ACTIVE" }, //
                { null, "DUNS1111", "ACTIVE" }, //
                { "dom2222.com", null, "ACTIVE" }, //
                { null, "DUNS3333", "ACTIVE" }, //
                { "dom11111.com", "DUNS11111", "OBSOLETE" }, //
                { "dom11111.com", "DUNS11111New", "ACTIVE" }, //
                { "dom11111.comNew", "DUNS11111", "ACTIVE" }, //
        };
        checkResult(records, expectedData, 34L);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
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
                { "dom11.com", null, "OBSOLETE" }, //
                { "dom33.com", null, "ACTIVE" }, //
                { "dom1111.com", "DUNS1111", "ACTIVE" }, //
                { "dom2222.com", "DUNS2222", "UPDATED" }, //
                { "dom3333.com", "DUNS3333", "UPDATED" }, //
                { "dom1111.com", null, "ACTIVE" }, //
                { null, "DUNS1111", "ACTIVE" }, //
                { "dom2222.com", null, "ACTIVE" }, //
                { null, "DUNS3333", "ACTIVE" }, //
                { "dom11111.com", "DUNS11111", "OBSOLETE" }, //
                { "dom11111.com", "DUNS11111New", "ACTIVE" }, //
                { "dom11111.comNew", "DUNS11111", "ACTIVE" }, //
        };
        checkResult(records, expectedData, 34L);
    }

    private void checkResult(Iterator<GenericRecord> records, Object[][] expectedData, long expectedRows) {
        Map<String, String> expected = new HashMap<>();
        for (Object[] data : expectedData) {
            expected.put(String.valueOf(data[0] + String.valueOf(data[1])), String.valueOf(data[2]));
        }

        List<GenericRecord> sorted = new ArrayList<>();
        records.forEachRemaining(sorted::add);
        sorted.sort(Comparator.comparing(r -> ((Long) r.get("LatticeID"))));

        int rowNum = 0;
        Set<Long> origIds = new HashSet<Long>();
        Set<Long> activeIds = new HashSet<Long>();

        for (GenericRecord record : sorted) {
            log.info(record.toString());
            Long id = (Long) record.get("LatticeID");
            Long redirectFromId = (Long) record.get("RedirectFromId");
            Assert.assertFalse(origIds.contains(redirectFromId));
            origIds.add(redirectFromId);
            Assert.assertEquals(String.valueOf(record.get("Status")),
                    expected.get(String.valueOf(record.get("Domain")) + String.valueOf(record.get("DUNS"))));
            Assert.assertNotNull(record.get("Status"));
            if ("ACTIVE".equals(record.get("Status").toString())) {
                Assert.assertFalse(activeIds.contains(id));
                activeIds.add(id);
            }
            rowNum++;
        }
        Assert.assertEquals(expectedRows, rowNum);
    }

}
