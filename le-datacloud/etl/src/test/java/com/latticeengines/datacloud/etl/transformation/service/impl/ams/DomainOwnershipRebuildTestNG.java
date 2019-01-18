package com.latticeengines.datacloud.etl.transformation.service.impl.ams;

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
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.DomainOwnershipRebuildFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DomainOwnershipRebuildTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DomainOwnershipRebuildTestNG.class);

    private static final String DOM_OWNERSHIP_TABLE = "DomainOwnershipTable";
    private static final String DOMSRC_DNB = DataCloudConstants.DOMSRC_DNB;

    private GeneralSource source = new GeneralSource(DOM_OWNERSHIP_TABLE);
    private GeneralSource ams = new GeneralSource("AccountMasterSeed");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareAmSeed();
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
            configuration.setName("DomainOwnershipRebuild");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(ams.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(DomainOwnershipRebuildFlow.TRANSFORMER_NAME);
            String confParamStr1 = getDomOwnershipTableConfig();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(source.getSourceName());

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDomOwnershipTableConfig() throws JsonProcessingException {
        DomainOwnershipConfig conf = new DomainOwnershipConfig();
        conf.setFranchiseThreshold(3);
        conf.setMultLargeCompThreshold(500000000L);
        return JsonUtils.serialize(conf);
    }

    private void prepareAmSeed() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN, String.class));
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_GU_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_DU_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_SALES_VOL_US, Long.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_EMPLOYEE_TOTAL, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS, Integer.class));
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_ALEXA_RANK, Integer.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN, String.class));
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN_SOURCE, String.class));
        Object[][] data = new Object[][] {
                // Case 1 : Domains present in domain ownership table with SINGLE_TREE type : result = not cleaned up
                	// Adding this entry present in single tree for ROOT_DUNS firmographic value computation : karlDuns2.com
                { "karlDu.com", "DUNS24", null, "DUNS24", 21100024L, "50000", 3, "Accounting", 202, null, DOMSRC_DNB },
                { "netappGu.com", "DUNS28", "DUNS28", null, 2250000262L, "55000", 20, "Passenger Car Leasing", 203,
                    null, DOMSRC_DNB },
                	// Adding this entry to support FRANCHISE case : sbiDuns1.com
                { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services", 201, null, null,
                        DOMSRC_DNB },
                { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production", 200, null,
                            DOMSRC_DNB },
                { "mongodbDu.com", "DUNS18", "DUNS17", "DUNS18", 510002421L, "22009", 9, null, 205, null, null,
                                DOMSRC_DNB },
                	// Adding this entry to support mongodbDu.com case
                { "mongodbGu.com", "DUNS17", "DUNS17", "DUNS18", 2250000242L, "67009", 34, "Legal", 206, null,
                                    DOMSRC_DNB },
                // Case 2 : domains present in OwnershipTable : rootDuns match
                { "karlDuns2.com", "DUNS34", "DUNS28", null, 304500L, "2200", 1, "Media", 215, null, DOMSRC_DNB },
                { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal", 216, null, DOMSRC_DNB },
                // Case 3 : domains present in OwnershipTable : rootDuns doesn't match
                { "karlDuns2.com", "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research", 218, null, DOMSRC_DNB },
                { "sbiDuns2.com", "DUNS01", null, "DUNS01", 21100024L, "50000", null, null, 223, null, DOMSRC_DNB },
                // Case 4 : domains present in OwnershipTable with reasons multiple large company, franchise, other
                { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services", 225, null,
                        DOMSRC_DNB },
                { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1,
                        "Manufacturing - Semiconductors", 226, null, DOMSRC_DNB },
                { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology", 227, null,
                        DOMSRC_DNB },
                { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production", 228, null,
                        DOMSRC_DNB },
                { "tesla.com", "DUNS111", "DUNS111", "DUNS110", 3131213L, "1123", 3, "Legal", 229, null, DOMSRC_DNB },
                { "tesla.com", "DUNS121", "DUNS121", "DUNS120", 3131213L, "1123", 3, "Legal", 230, null, DOMSRC_DNB },
                { "tesla.com", "DUNS122", "DUNS122", null, 3131213L, "1123", 3, "Legal", 231, null, DOMSRC_DNB },
                // Case 5 : domain only entries
                { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes", 233, null,
                        DOMSRC_DNB },
                // Case 6 : duns only entries
                { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services", 234, null,
                        DOMSRC_DNB },
                // Case 7 : domains with missing ROOT_DUNS
                { "netsuite.com", "DUNS890", "DUNS900", null, 32847L, "4547", 13, "Media", 236, null, DOMSRC_DNB },};
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, data);
    }

    Object[][] expectedDataValues = new Object[][] { //
            // Domain, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER, REASON_TYPE, IS_NON_PROFITABLE
    		// SINGLE TREE : not cleaned up
    		{ "sbiGu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false" }, //
    		{ "sbiDu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false"}, //
    		{ "karlDu.com", "DUNS24", "DU", 1, "SINGLE_TREE", "false"}, //
    		{ "netappGu.com", "DUNS28", "GU", 1, "SINGLE_TREE", "false" }, //
    		{ "mongodbGu.com", "DUNS17", "GU", 1, "SINGLE_TREE", "false" }, //
    		{ "mongodbDu.com", "DUNS17", "GU", 1, "SINGLE_TREE", "false" }, //
    		// FRANCHISE : not cleaned up
    		{ "sbiDuns1.com", null, null, 4, "FRANCHISE", "false" }, //
    		// OTHER : not cleaned up
    		{ "tesla.com", null, null, 3, "OTHER", "false" }, //
    		// Reasons : HIGHER_NUM_OF_LOC, HIGHER_SALES_VOLUME := Cleaned up
            { "sbiDuns2.com", "DUNS10", "GU", 2, "HIGHER_NUM_OF_LOC", "false" }, //
            { "karlDuns2.com", "DUNS28", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
            // Missing root DUNS entry case (in single or multiple trees)
            // rootDuns = DUNS900
            { "netsuite.com", null, null, 1, "MISSING_ROOT_DUNS", "false" }, //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = null;
        expectedData = new HashMap<>();
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
            expectedData.remove(domain);
        }
        Assert.assertTrue(expectedData.size() == 0);
        Assert.assertEquals(rowCount, 11);
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }
}
