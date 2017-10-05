package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedDeriveAttrs;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedDeriveAttrsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMSeedDeriveAttrsTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("AccountMasterSeedEnriched");

    GeneralSource baseSource1 = new GeneralSource("AccountMasterSeed");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareAmSeedDuns();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
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
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AmSeedEnrichDuGuParentSales");
            configuration.setVersion(targetVersion);

            // Initialize manualSeed Data Set
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep1 = new ArrayList<String>();
            baseSourceStep1.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep1);
            step1.setTargetSource(source.getSourceName());
            step1.setTransformer(AMSeedDeriveAttrs.TRANSFORMER_NAME);
            String confParamStr = getAmSeedDataConfig();
            step1.setConfiguration(confParamStr);

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

    private String getAmSeedDataConfig() throws JsonProcessingException {
        AMSeedDeriveAttrsConfig conf = new AMSeedDeriveAttrsConfig();
        conf.setAmSeedDuns("DUNS");
        conf.setUsSalesVolume("SALES_VOLUME_US_DOLLARS");
        conf.setAmSeedDuDuns("DOMESTIC_ULTIMATE_DUNS_NUMBER");
        conf.setAmSeedGuDuns("GLOBAL_ULTIMATE_DUNS_NUMBER");
        conf.setAmSeedParentDuns("PARENT_ULTIMATE_DUNS_NUMBER");
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private Object[][] expectedDataValues = new Object[][] { //
            { 1000L, "DUNS2", "DUNS2", "DUNS1", 12500002422L, "sbi.com", "DUNS3", 12501111122L, 6666666662L,
                    12501111122L },
            { 11000L, "DUNS9", null, "DUNS9", null, null, null, null, null, null },
            { 10000L, "DUNS9", "DUNS9", "DUNS8", 1111111111L, "databrick.com", "DUNS6", null, 2324324222L, null },
            { 2000L, "DUNS2", "DUNS3", "DUNS3", 6666666662L, "sbiGu.com", "DUNS3", 12501111122L, 6666666662L,
                    6666666662L },
            { 3000L, "DUNS2", "DUNS3", "DUNS2", 12501111122L, "sbiDu.com", "DUNS3", 12501111122L, 6666666662L,
                    6666666662L },
            { 5000L, "DUNS6", "DUNS5", "DUNS5", 32321112322L, "teslaGu.com", "DUNS5", 2324324222L, 32321112322L,
                    32321112322L },
            { 6000L, "DUNS6", "DUNS5", "DUNS6", 2324324222L, "teslaDu.com", "DUNS5", 2324324222L, 32321112322L,
                    32321112322L },
            { 8000L, "DUNS8", null, "DUNS8", 1111111111L, null, null, 1111111111L, null, null },
            { 4000L, "DUNS6", "DUNS6", "DUNS4", 1111112422L, "tesla.com", "DUNS5", 2324324222L, 32321112322L, 2324324222L },
            { 7000L, "DUNS8", "DUNS8", "DUNS7", 4444442422L, "netapp.com", null, 1111111111L, null, 1111111111L },
            { 9000L, null, null, null, 2121314121L, "abc.com", null, null, null, null },
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowNum = 0;
        Map<Long, Object[]> expectedData = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            expectedData.put((Long) data[0], data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long latticeId = (Long) (record.get("LatticeId"));
            Object[] expected = expectedData.get(latticeId);
            Assert.assertTrue(isObjEquals(record.get("DOMESTIC_ULTIMATE_DUNS_NUMBER"), expected[1]));
            Assert.assertTrue(isObjEquals(record.get("PARENT_ULTIMATE_DUNS_NUMBER"), expected[2]));
            Assert.assertTrue(isObjEquals(record.get("DUNS"), expected[3]));
            Assert.assertTrue(isObjEquals(record.get("SALES_VOLUME_US_DOLLARS"), expected[4]));
            Assert.assertTrue(isObjEquals(record.get("Domain"), expected[5]));
            Assert.assertTrue(isObjEquals(record.get("GLOBAL_ULTIMATE_DUNS_NUMBER"), expected[6]));
            Assert.assertTrue(isObjEquals(record.get("DOMESTIC_HQ_SALES_VOLUME"), expected[7]));
            Assert.assertTrue(isObjEquals(record.get("GLOBAL_HQ_SALES_VOLUME"), expected[8]));
            Assert.assertTrue(isObjEquals(record.get("PARENTS_SALES_VOLUME"), expected[9]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 11);
    }

    private void prepareAmSeedDuns() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeId", Long.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        columns.add(Pair.of("DOMESTIC_ULTIMATE_DUNS_NUMBER", String.class));
        columns.add(Pair.of("GLOBAL_ULTIMATE_DUNS_NUMBER", String.class));
        columns.add(Pair.of("PARENT_ULTIMATE_DUNS_NUMBER", String.class));
        Object[][] data = new Object[][] {
                { 1000L, "DUNS1", "sbi.com", 12500002422L, "DUNS2", "DUNS3", "DUNS2" },
                { 2000L, "DUNS3", "sbiGu.com", 6666666662L, "DUNS2", "DUNS3", "DUNS3" },
                { 3000L, "DUNS2", "sbiDu.com", 12501111122L, "DUNS2", "DUNS3", "DUNS3" },
                { 4000L, "DUNS4", "tesla.com", 1111112422L, "DUNS6", "DUNS5", "DUNS6" },
                { 5000L, "DUNS5", "teslaGu.com", 32321112322L, "DUNS6", "DUNS5", "DUNS5" },
                { 6000L, "DUNS6", "teslaDu.com", 2324324222L, "DUNS6", "DUNS5", "DUNS5" },
                { 7000L, "DUNS7", "netapp.com", 4444442422L, "DUNS8", null, "DUNS8" },
                { 8000L, "DUNS8", null, 1111111111L, "DUNS8", null, null },
                { 9000L, null, "abc.com", 2121314121L, null, null, null },
                { 10000L, "DUNS8", "databrick.com", 1111111111L, "DUNS9", "DUNS6", "DUNS9" },
                { 11000L, "DUNS9", null, null, "DUNS9", null, null } };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, columns, data);
    }

}
