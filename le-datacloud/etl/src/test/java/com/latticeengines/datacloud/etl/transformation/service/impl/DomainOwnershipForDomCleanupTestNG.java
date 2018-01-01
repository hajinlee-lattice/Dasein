package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.FormDomainOwnershipTableFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DomainOwnershipForDomCleanupTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("DomainOwnershipTable");
    GeneralSource baseSource1 = new GeneralSource("AccountMasterSeed");
    GeneralSource baseSource2 = new GeneralSource("OrbCacheSeedSecondaryDomain");
    private static final Logger log = LoggerFactory.getLogger(DomainOwnershipForDomCleanupTestNG.class);

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareAmSeed();
        prepareOrbSeedSecondaryDom();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareAmSeed() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("LDC_Domain", String.class));
        schema.add(Pair.of("LDC_DUNS", String.class));
        schema.add(Pair.of("GLOBAL_ULTIMATE_DUNS_NUMBER", String.class));
        schema.add(Pair.of("DOMESTIC_ULTIMATE_DUNS_NUMBER", String.class));
        schema.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));
        schema.add(Pair.of("EMPLOYEES_TOTAL", String.class));
        schema.add(Pair.of("LE_NUMBER_OF_LOCATIONS", Integer.class));
        Object[][] data = new Object[][] { { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 1250000242L, "50000", 60 },
                { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30 },
                { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2 },
                { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3 },
                { "mongodbGu.com", "DUNS17", "DUNS17", "DUNS18", 2250000242L, "67009", 34 },
                { "mongodbDu.com", "DUNS18", "DUNS17", "DUNS18", 510002421L, "22009", 9 },
                { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1 },
                { "karlGu.com", null, null, null, null, null, null },
                { "karlDu.com", "DUNS24", null, "DUNS24", 21100024L, "3000", 3 },
                { "karlDuns1.com", "DUNS26", null, "DUNS24", 30191910L, "1001", 1 },
                { "karlDuns2.com", "DUNS27", null, "DUNS24", 30450010L, "220", 2 },
                { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1 },
                { "netappGu.com", "DUNS28", "DUNS28", null, 5000000010L, "55000", 20 },
                { "netappDu.com", null, "DUNS28", null, null, null, null },
                { "netappDuns1.com", "DUNS31", "DUNS28", null, 30450010L, "10000", 3 },
                { "netappDuns2.com", "DUNS33", "DUNS28", null, 30450010L, "8000", 3 },
                { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2 },
                { "mongoDbDuns1.com", "DUNS21", "DUNS17", "DUNS18", 30450010L, "10000", 1 } };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareOrbSeedSecondaryDom() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("SecondaryDomain", String.class));
        schema.add(Pair.of("PrimaryDomain", String.class));
        Object[][] data = new Object[][] { { "sbiDuns2.com", "karlDuns1.com" }, { "karlDuns2.com", "netappDuns1.com" },
                { "datos.com", "intuit.com" }, { "apple.com", "uber.com" },
                { "netappDuns2.com", "mongoDbDuns1.com" }, { "karlDuns1.com", "netappDuns2.com" },
                { "craigslist.com", "netappDuns1.com" }, { "target.com", "macys.com" },
                { "karlDuns2.com", "oldnavy.com" }, { "amazon.com", "mongoDbDuns1.com" },
                { "amazon.com", "netappDuns1.com" } };
        uploadBaseSourceData(baseSource2.getSourceName(), baseSourceVersion, schema, data);
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
            configuration.setName("FormDomainOwnershipTable");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            baseSourceStep.add(baseSource2.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(FormDomainOwnershipTableFlow.TRANSFORMER_NAME);
            String confParamStr1 = getDomOwnershipTableConfig();
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

    private String getDomOwnershipTableConfig() throws JsonProcessingException {
        FormDomOwnershipTableConfig conf = new FormDomOwnershipTableConfig();
        conf.setAmSeedDomain("LDC_Domain");
        conf.setAmSeedDuns("LDC_DUNS");
        conf.setOrbSecPriDom("PrimaryDomain");
        conf.setOrbSrcSecDom("SecondaryDomain");
        conf.setAmSeedDuDuns("DOMESTIC_ULTIMATE_DUNS_NUMBER");
        conf.setAmSeedGuDuns("GLOBAL_ULTIMATE_DUNS_NUMBER");
        conf.setUsSalesVolume("SALES_VOLUME_US_DOLLARS");
        conf.setTotalEmp("EMPLOYEES_TOTAL");
        conf.setNumOfLoc("LE_NUMBER_OF_LOCATIONS");
        conf.setFranchiseThreshold(3);
        conf.setMultLargeCompThreshold(500000000L);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    Object[][] expectedDataValues = new Object[][] { //
            { "karlDuns1.com", "DUNS33", "DUNS", 2, "HIGHER_SALES_VOLUME" }, //
            { "karlDuns2.com", "DUNS31", "DUNS", 2, "HIGHER_EMP_TOTAL" }, //
            { "sbiDuns2.com", "DUNS14", "DUNS", 2, "MULTIPLE_LARGE_COMPANY" }, //
            { "amazon.com", "DUNS31", "DUNS", 2, "HIGHER_NUM_OF_LOC" }, //
            { "netappDuns2.com", "DUNS21", "DUNS", 2, "HIGHER_EMP_TOTAL" }, //
            { "sbiDuns1.com", "DUNS13,DUNS20,DUNS29,DUNS66", "DUNS", 4, "FRANCHISE" } //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            expectedData.put(String.valueOf(data[0]) + String.valueOf(data[4]), data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : " + record);
            String domain = String.valueOf(record.get(0));
            String reasonType = String.valueOf(record.get(4));
            Object[] expected = expectedData.get(domain + reasonType);
            Assert.assertTrue(isObjEquals(record.get(0), expected[0]));
            if(String.valueOf(expected[1]).contains(",")) {
                String[] dunsValues = String.valueOf(expected[1]).split(",");
                String dunsVal = String.valueOf(record.get(1));
                Object isPresent = "false";
                for(int i = 0; i < dunsValues.length; i++) {
                    if (dunsValues[i].equals(dunsVal)) {
                        isPresent = "true";
                    }
                }
                Assert.assertTrue(isObjEquals(isPresent, "true"));
            } else {
                Assert.assertTrue(isObjEquals(record.get(1), expected[1]));
            }
            Assert.assertTrue(isObjEquals(record.get(2), expected[2]));
            Assert.assertTrue(isObjEquals(record.get(3), expected[3]));
            Assert.assertTrue(isObjEquals(record.get(4), expected[4]));
            rowCount++;
        }
        Assert.assertEquals(rowCount, 6);
    }

}
