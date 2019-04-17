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
import com.latticeengines.datacloud.dataflow.transformation.AMSeedDedup;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AMSeedDedupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMSeedDedupTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource baseSource1 = new GeneralSource("AccountMasterSeed");
    GeneralSource source = new GeneralSource("amSeedDeduped");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareAmSeed();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareAmSeed() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("Domain", String.class));
        schema.add(Pair.of("DUNS", String.class));
        schema.add(Pair.of("GLOBAL_ULTIMATE_DUNS_NUMBER", String.class));
        schema.add(Pair.of("LE_PRIMARY_DUNS", String.class));
        schema.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        schema.add(Pair.of("EMPLOYEES_TOTAL", String.class));
        schema.add(Pair.of("LE_NUMBER_OF_LOCATIONS", Integer.class));
        schema.add(Pair.of("PrimaryIndustry", String.class));
        Object[][] data = new Object[][] {
                // domain only case with another entry present
                { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production" },
                { "sbiGu.com", "DUNS12", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production" },
                { "sbiGu.com", null, "DUNS66", "DUNS13", 214245L, "23123", 12, "Accounting" },
                // single entry of domain only record
                { "mcDonalds.com", null, "DUNS28", "DUNS33", 139193L, "1378", 28, "Media" },
                // duns only case with another entry present
                { "velocity.com", "DUNS26", "DUNS8", null, 131314L, "232", 1, "Media" },
                { "velocity.com", "DUNS28", "DUNS9", null, 312312L, "131", 1, "Media" },
                { null, "DUNS26", null, "DUNS24", 30191910L, "1001", 1, "Accounting" },
                // single entry of duns only record
                { null, "DUNS29", "DUNS98", "DUNS09", 1783718L, "172", 2, "Food Production" },
                // dedup case
                { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services" },
                { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services" } };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, schema, data);
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
            configuration.setName("AmSeedDeduper");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(AMSeedDedup.TRANSFORMER_NAME);
            String confParamStr1 = getAmSeedDedupConfig();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(source.getSourceName());

            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getAmSeedDedupConfig() throws JsonProcessingException {
        AMSeedDedupConfig conf = new AMSeedDedupConfig();
        conf.setIsDunsOnlyCleanup(true);
        conf.setIsDomainOnlyCleanup(true);
        conf.setIsDedup(true);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    Object[][] expectedDataValues = new Object[][] { //
            // domain, DUNS, GU, DU, salesVolume, empTotal, numOfLoc, primInd
            // single entry of domain only record
            { "mcDonalds.com", null, "DUNS28", "DUNS33", 139193L, "1378", 28, "Media" }, //
            // dedup case
            { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services" }, //
            // duns only case with another entry present
            { "velocity.com", "DUNS26", "DUNS8", null, 131314L, "232", 1, "Media" }, //
            { "velocity.com", "DUNS28", "DUNS9", null, 312312L, "131", 1, "Media" }, //
            // domain only case with another entry present
            { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production" }, //
            { "sbiGu.com", "DUNS12", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production" }, //
            // single entry of duns only record
            { null, "DUNS29", "DUNS98", "DUNS09", 1783718L, "172", 2, "Food Production" } //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> amSeedExpectedValues = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            amSeedExpectedValues.put(String.valueOf(data[0]) + String.valueOf(data[1]), data);
        }
        String[] expectedValueOrder = { "Domain", "DUNS", "GLOBAL_ULTIMATE_DUNS_NUMBER", "LE_PRIMARY_DUNS",
                "SALES_VOLUME_US_DOLLARS", "EMPLOYEES_TOTAL", "LE_NUMBER_OF_LOCATIONS", "PrimaryIndustry" };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String domain = String.valueOf(record.get("Domain"));
            String duns = String.valueOf(record.get("DUNS"));
            Object[] expectedVal = amSeedExpectedValues.get(domain + duns);
            for (int i = 0; i < expectedValueOrder.length; i++) {
                Assert.assertTrue(
                        isObjEquals(record.get(expectedValueOrder[i]), expectedVal[i]));
            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, 7);
    }

}
