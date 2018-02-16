package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.datacloud.dataflow.transformation.CleanupAmSeedSrcFlow;
import com.latticeengines.datacloud.dataflow.transformation.DomainOwnershipRebuildFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DomainOwnershipForDomCleanupTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource domOwnTable = new GeneralSource("DomainOwnershipTable");
    GeneralSource amSeedCleanup = new GeneralSource("AccountMasterSeedCleanedUp");
    GeneralSource source = new GeneralSource("OrbSecSrcCleanedUp");
    GeneralSource baseSource1 = new GeneralSource("AccountMasterSeed");
    GeneralSource baseSource2 = new GeneralSource("OrbCacheSeedSecondaryDomain");
    private static final String DOM_OWNERSHIP_TABLE = "DomainOwnershipTable";
    private static final String ACC_MASTER_SEED_CLEANUP = "AccountMasterSeedCleanedUp";
    private static final Logger log = LoggerFactory.getLogger(DomainOwnershipForDomCleanupTestNG.class);

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareAmSeed();
        prepareOrbSeedSecondaryDom();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(domOwnTable, targetVersion);
        //confirmIntermediateSource(amSeedCleanup, targetVersion);
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
        schema.add(Pair.of("LDC_PrimaryIndustry", String.class));
        Object[][] data = new Object[][] {
                // domains not present in domainOwnershipTable
                { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production" },
                { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services" },
                { "karlDu.com", "DUNS24", null, "DUNS24", 21100024L, "50000", 3, "Accounting" },
                { "netappGu.com", "DUNS28", "DUNS28", null, 2250000262L, "55000", 20, "Passenger Car Leasing" },
                { "amazonGu.com", "DUNS36", "DUNS36", null, 3250000242L, "11000", 2, "Energy" },
                { "mongodbDu.com", "DUNS18", "DUNS17", "DUNS18", 510002421L, "22009", 9, null },
                { "mongodbGu.com", "DUNS17", "DUNS17", "DUNS18", 2250000242L, "67009", 34, "Legal" },
                { "regalGoodWill.com", "DUNS55", "DUNS55", null, 9728329L, "2230", 11, "Media" },
                { "goodWillOrg.com", "DUNS59", "DUNS59", null, 82329840L, "2413", 10, "Media" },
                { "netappDuns1.com", "DUNS31", "DUNS28", null, 30450010L, "10000", 3, "Junior Colleges" },
                { "mongoDbDuns1.com", "DUNS21", "DUNS17", "DUNS18", 30450010L, "10000", 1, "Wholesale" },
                { "worldwildlife.org", "DUNS06", "DUNS39", null, 204500L, "1500", 1, "Government" },
                { "wordwildlifeGu.org", "DUNS39", "DUNS39", "DUNS38", 304500L, "3700", 3, "Education" },
                { "socialorg.com", "DUNS54", null, null, 94500L, "98924", 2, "Education" },
                // domains present in OwnershipTable : rootDuns match
                { "karlDuns2.com", "DUNS34", "DUNS28", null, 304500L, "2200", 1, "Media" },
                { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal" },
                // domains present in OwnershipTable : rootDuns doesnt match
                { "karlDuns1.com", "DUNS26", null, "DUNS24", 30191910L, "1001", 1, "Accounting" },
                { "karlDuns2.com", "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research" },
                { "netappDuns2.com", "DUNS33", null, null, 30450010L, "8000", 3, "Biotechnology" },
                { "unicef.org", "DUNS22", null, null, 104500L, "3700", 2, "Non-profit" },
                { "goodwill.com", "DUNS53", "DUNS55", null, 8502491L, "1232", 2, "Media" },
                { "goodwill.com", "DUNS79", null, "DUNS59", 9502492L, "2714", 2, "Media" },
                { "sbiDuns2.com", "DUNS01", null, "DUNS01", 21100024L, "50000", null, null },
                // domains present in OwnershipTable with reasons multiple large
                // company, franchise
                { "amazon.com", "DUNS37", "DUNS36", null, null, "2200", 1, "Media" },
                { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services" },
                { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1,
                        "Manufacturing - Semiconductors" },
                { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology" },
                { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production" },
                // domain only entries
                { "amazon.com", null, "DUNS17", "DUNS18", 100002421L, null, 1, "Manufacturing - Semiconductors" },
                { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes" },
                // duns only entries
                { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services" },
                { null, "DUNS69", null, "DUNS69", 231131L, "1313", 2, "Non-profit" }
        };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareOrbSeedSecondaryDom() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("SecondaryDomain", String.class));
        schema.add(Pair.of("PrimaryDomain", String.class));
        Object[][] data = new Object[][] { { "sbiDuns2.com", "karlDuns1.com" }, { "karlDuns2.com", "netappDuns1.com" },
                { "datos.com", "intuit.com" }, { "apple.com", "uber.com" }, { "unicef.org", "worldwildlife.org" },
                { "goodwill.com", "socialorg.com" }, { "netappDuns2.com", "mongoDbDuns1.com" },
                { "karlDuns1.com", "netappDuns2.com" }, { "craigslist.com", "netappDuns1.com" },
                { "target.com", "macys.com" },
                { "karlDuns2.com", "oldnavy.com" }, { "amazon.com", "mongoDbDuns1.com" },
                { "amazon.com", "netappDuns1.com" }, { "dell.com", "netappDuns3.com" }, { "airbnb.com", "lyft.com" } };
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
            step1.setTransformer(DomainOwnershipRebuildFlow.TRANSFORMER_NAME);
            String confParamStr1 = getDomOwnershipTableConfig();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(domOwnTable.getSourceName());

            // -----------------
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<String> cleanupAmSeedSrc = new ArrayList<String>();
            List<Integer> cleanupAmSeedStep = new ArrayList<Integer>();
            cleanupAmSeedStep.add(0);
            cleanupAmSeedSrc.add(baseSource1.getSourceName());
            step2.setInputSteps(cleanupAmSeedStep);
            step2.setBaseSources(cleanupAmSeedSrc);
            step2.setTransformer(CleanupAmSeedSrcFlow.TRANSFORMER_NAME);
            step2.setConfiguration(confParamStr1);
            step2.setTargetSource(source.getSourceName());

            /*
            // -----------------
            TransformationStepConfig step3 = new TransformationStepConfig();
            List<Integer> cleanupOrbSecSrcStep = new ArrayList<Integer>();
            List<String> cleanupOrbSecSrc = new ArrayList<String>();
            cleanupOrbSecSrcStep.add(0);
            cleanupOrbSecSrc.add(baseSource2.getSourceName());
            cleanupOrbSecSrc.add(baseSource1.getSourceName());
            step3.setInputSteps(cleanupOrbSecSrcStep);
            step3.setBaseSources(cleanupOrbSecSrc);
            step3.setTransformer(CleanupOrbSecSrcFlow.TRANSFORMER_NAME);
            step3.setConfiguration(confParamStr1);
            step3.setTargetSource(source.getSourceName());
            */

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            //steps.add(step3);

            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDomOwnershipTableConfig() throws JsonProcessingException {
        FormDomOwnershipTableConfig conf = new FormDomOwnershipTableConfig();
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
            { "karlDuns1.com", "DUNS33", "DUNS", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "karlDuns2.com", "DUNS28", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "sbiDuns2.com", "DUNS10", "GU", 3, "HIGHER_NUM_OF_LOC", "false" }, //
            { "amazon.com", null, null, 3, "MULTIPLE_LARGE_COMPANY", "false" }, //
            { "netappDuns2.com", "DUNS17", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "sbiDuns1.com", null, null, 4, "FRANCHISE", "false" }, //
            { "unicef.org", "DUNS39", "GU", 2, "HIGHER_NUM_OF_LOC", "true" }, //
            { "goodwill.com", "DUNS54", "DUNS", 3, "HIGHER_EMP_TOTAL", "true" }
    };

    Object[][] amSeedCleanedUpValues = new Object[][] { //
            // domains not present in OwnershipTable : result = domain not
            // cleaned up
            { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production"},
            { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services"},
            { "karlDu.com", "DUNS24", null, "DUNS24", 21100024L, "50000", 3, "Accounting"},
            { "netappGu.com", "DUNS28", "DUNS28", null, 2250000262L, "55000", 20, "Passenger Car Leasing" },
            { "amazonGu.com", "DUNS36", "DUNS36", null, 3250000242L, "11000", 2, "Energy"},
            { "mongodbDu.com", "DUNS18", "DUNS17", "DUNS18", 510002421L, "22009", 9, null},
            { "mongodbGu.com", "DUNS17", "DUNS17", "DUNS18", 2250000242L, "67009", 34, "Legal"},
            { "regalGoodWill.com", "DUNS55", "DUNS55", null, 9728329L, "2230", 11, "Media" },
            { "goodWillOrg.com", "DUNS59", "DUNS59", null, 82329840L, "2413", 10, "Media"},
            { "netappDuns1.com", "DUNS31", "DUNS28", null, 30450010L, "10000", 3, "Junior Colleges"},
            { "mongoDbDuns1.com", "DUNS21", "DUNS17", "DUNS18", 30450010L, "10000", 1, "Wholesale"},
            { "worldwildlife.org", "DUNS06", "DUNS39", null, 204500L, "1500", 1, "Government"},
            { "wordwildlifeGu.org", "DUNS39", "DUNS39", "DUNS38", 304500L, "3700", 3, "Education" },
            { "socialorg.com", "DUNS54", null, null, 94500L, "98924", 2, "Education" },
            // domains present in OwnershipTable (rootDuns match) : result =
            // domain not cleaned up
            { "karlDuns2.com", "DUNS34", "DUNS28", null, 304500L, "2200", 1, "Media"},
            { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal"},
            // domains present in OwnershipTable (rootDuns doesn't match) :
            // result = domain cleaned up
            { null, "DUNS26", null, "DUNS24", 30191910L, "1001", 1, "Accounting"},
            { null, "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research" },
            { null, "DUNS33", null, null, 30450010L, "8000", 3, "Biotechnology"},
            { null, "DUNS22", null, null, 104500L, "3700", 2, "Non-profit" },
            { null, "DUNS53", "DUNS55", null, 8502491L, "1232", 2, "Media"},
            { null, "DUNS79", null, "DUNS59", 9502492L, "2714", 2, "Media"},
            { null, "DUNS01", null, "DUNS01", 21100024L, "50000", null, null },
            // domains present in OwnershipTable with reasons multiple large
            // company, franchise : result = not cleaned up
            { "amazon.com", "DUNS37", "DUNS36", null, null, "2200", 1, "Media" },
            { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services"},
            { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1, "Manufacturing - Semiconductors"},
            { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology"},
            { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production"},
            // domain only entries : not cleaned up
            { "amazon.com", null, "DUNS17", "DUNS18", 100002421L, null, 1, "Manufacturing - Semiconductors"},
            { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes"},
            // duns only entries : not cleaned up
            { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services"},
            { null, "DUNS69", null, "DUNS69", 231131L, "1313", 2, "Non-profit"},
    };

    Object[][] orbSecSrcCleanedupValues = new Object[][] { //
            { "lyft.com", "airbnb.com" }, { "netappDuns2.com", "karlDuns1.com" },
            { "netappDuns1.com", "karlDuns2.com" }, { "uber.com", "apple.com" }, { "intuit.com", "datos.com" },
            { "netappDuns1.com", "craigslist.com" }, { "mongoDbDuns1.com", "netappDuns2.com" },
            { "netappDuns3.com", "dell.com" }, { "macys.com", "target.com" }, { "netappDuns1.com", "amazon.com" },
            { "mongoDbDuns1.com", "amazon.com" } };

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        int rowCount = 0;
        switch (source) {
            case DOM_OWNERSHIP_TABLE:
                rowCount = 0;
                Map<String, Object[]> expectedData = new HashMap<>();
                for (Object[] data : expectedDataValues) {
                    expectedData.put(String.valueOf(data[0]), data);
                }
                while (records.hasNext()) {
                    GenericRecord record = records.next();
                    log.info("record : " + record);
                    String domain = String.valueOf(record.get(0));
                    Object[] expected = expectedData.get(domain);
                    Assert.assertTrue(isObjEquals(record.get(0), expected[0]));
                    Assert.assertTrue(isObjEquals(record.get(1), expected[1]));
                    Assert.assertTrue(isObjEquals(record.get(2), expected[2]));
                    Assert.assertTrue(isObjEquals(record.get(3), expected[3]));
                    Assert.assertTrue(isObjEquals(record.get(4), expected[4]));
                    Assert.assertTrue(isObjEquals(record.get(5), expected[5]));
                    rowCount++;
                }
                Assert.assertEquals(rowCount, 8);
                break;
                /*
            case ACC_MASTER_SEED_CLEANUP:
                break;*/
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        rowCount = 0;
        List<Integer> attrNameIndex = Arrays.asList(6, 1, 7, 0, 5, 2, 3, 4);
        Map<String, Object[]> amSeedExpectedValues = new HashMap<>();
        for (Object[] data : amSeedCleanedUpValues) {
            amSeedExpectedValues.put(String.valueOf(data[0]) + String.valueOf(data[1]), data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : " + record);
            String domain = String.valueOf(record.get(attrNameIndex.get(0)));
            String duns = String.valueOf(record.get(attrNameIndex.get(1)));
            Object[] expectedVal = amSeedExpectedValues.get(domain + duns);
            int columnCounter = 0;
            int totalColumns = 8;
            while (columnCounter < totalColumns) {
                Assert.assertTrue(
                        isObjEquals(record.get(attrNameIndex.get(columnCounter)), expectedVal[columnCounter]));
                columnCounter++;
            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, 32);
    }
}
