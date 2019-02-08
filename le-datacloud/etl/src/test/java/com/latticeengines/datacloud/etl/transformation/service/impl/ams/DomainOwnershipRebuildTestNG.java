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
import com.latticeengines.datacloud.dataflow.transformation.ams.DomainOwnershipRebuild;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.ams.DomainOwnershipConfig;
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
            step1.setTransformer(DomainOwnershipRebuild.TRANSFORMER_NAME);
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
                // Case 1 : Domains present in domain ownership table with
                // SINGLE_TREE type : result = not cleaned up
                // Present in single tree for ROOT_DUNS = DUNS24 firmographic
                // value computation : Case 2 karlDuns2.com and Case 3
                // sbiDuns1.com
                { "karlDu.com", "DUNS24", null, "DUNS24", 21100024L, "50000", 3, "Accounting", 202, null, DOMSRC_DNB },
                // Present in single tree for ROOT_DUNS = DUNS28 firmographic
                // value computation : Case 2 karlDuns2.com and Case 3
                // sbiDuns1.com
                { "netappGu.com", "DUNS28", "DUNS28", null, 2250000262L, "55000", 20, "Passenger Car Leasing", 203,
                        null, DOMSRC_DNB },
                // Present in single tree for ROOT_DUNS = DUNS10 firmographic
                // value computation : Case 2 sbiDuns2.com and Case 3
                // sbiDuns1.com
                { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production", 200, null,
                        DOMSRC_DNB },
                // Present in single tree for ROOT_DUNS = DUNS77 firmographic
                // value computation : Case 2 vlocityDuns2.com
                { "vlocityGu.com", "DUNS77", "DUNS77", null, 9250040L, "9432", 13, "Legal", 131, null, DOMSRC_DNB },
                // Present in single tree for ROOT_DUNS = DUNS82 firmographic
                // value computation : Case 2 vlocityDuns2.com
                { "rakutenGu.com", "DUNS82", "DUNS82", null, 9250040L, "7482", 11, "Media", 111, null, DOMSRC_DNB },
                // Adding this entry to support mongodbDu.com case : Case 3
                // sbiDuns1.com
                { "mongodbGu.com", "DUNS17", "DUNS17", "DUNS18", 2250000242L, "67009", 34, "Legal", 206, null,
                        DOMSRC_DNB },
                // Adding this entry to support craigslist.com in Case 3
                // craigslist.com
                { "craigsGu.com", "DUNS61", "DUNS61", null, 5020405000L, "4324", 23, "Media", 111, null, DOMSRC_DNB },
                { "craigsDu.com", "DUNS62", null, "DUNS62", 5060405000L, "4114", 21, "Food Production", 411, null,
                        DOMSRC_DNB },
                // Case 2 : domains present in OwnershipTable : rootDuns match -
                // we select root duns for domains present in multiple trees by
                // comparing their firmographic values in following order (Sales
                // Volume -> Total Employees -> Number of Locations) and by this
                // process we have following entries win over other entries
                // having same domain under other trees

                // karlDuns2.com domain record with DUNS = DUNS28 wins due to
                // reason = HIGHER_SALES_VOLUME
                { "karlDuns2.com", "DUNS34", "DUNS28", null, 304500L, "2200", 1, "Media", 215, null, DOMSRC_DNB },
                // sbiDuns2.com domain record with DUNS = DUNS10 wins due to
                // reason = HIGHER_NUM_OF_LOC(same salesVolume and
                // totalEmployees)
                { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal", 216, null, DOMSRC_DNB },
                // vlocityDuns2.com domain record with DUNS = DUNS78 wins due to
                // reason = HIGHER_EMP_TOTAL(same salesVolume)
                { "vlocityDuns2.com", "DUNS78", "DUNS77", "DUNS79", 40002499L, "4552", 2, "Media", 342, null,
                        DOMSRC_DNB },

                // Following domains present in OwnershipTable : rootDuns
                // doesn't
                // match = these are the entries that will get cleaned up as
                // same domain
                // in other tree (other root duns) got selected as provided
                // above
                // karlDuns2.com domain record with DUNS = DUNS24 loses due to
                // lower sales volume
                { "karlDuns2.com", "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research", 218, null, DOMSRC_DNB },
                // sbiDuns2.com domain record with DUNS = DUNS01 loses due to
                // lower num of locations(same root duns salesVolume and
                // totalEmployees)
                { "sbiDuns2.com", "DUNS01", null, "DUNS01", 21100024L, "50000", null, null, 223, null, DOMSRC_DNB },
                // vlocityDuns2.com domain record with DUNS = DUNS82 loses due
                // to lower total employees(same root duns salesVolume)
                { "vlocityDuns2.com", "DUNS88", "DUNS82", "DUNS80", 70002499L, "6552", 4, "Legal", 123, null,
                        DOMSRC_DNB },
                // Case 3 : domains present in OwnershipTable with reasons
                // multiple large company, franchise, other (domains present in
                // multiple trees having firmographics same in all the trees)
                // sbiDuns1.com is franchise and craigslist.com is multi-large
                // companies
                { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services", 225, null,
                        DOMSRC_DNB },
                { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1,
                        "Manufacturing - Semiconductors", 226, null, DOMSRC_DNB },
                { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology", 227, null,
                        DOMSRC_DNB },
                { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production", 228, null,
                        DOMSRC_DNB },
                { "craigslist.com", "DUNS07", "DUNS61", null, 6660405L, "23552", 2, "Legal", 31, null, DOMSRC_DNB },
                { "craigslist.com", "DUNS03", null, "DUNS62", 5020405L, "2123", 2, "Media", 142, null, DOMSRC_DNB },

                // ReasonType = OTHER
                // Added reason_type = 'OTHER' for domains present in multiple
                // trees having firmographics same in both trees.
                // So, can't choose the tree, so will update ROOT_DUNS = null
                // and DUNS_TYPE = null for such entries.
                // So, such entries will not be cleaned up
                { "tesla.com", "DUNS111", "DUNS111", "DUNS110", 3131213L, "1123", 3, "Legal", 229, null, DOMSRC_DNB },
                { "tesla.com", "DUNS121", "DUNS121", "DUNS120", 3131213L, "1123", 3, "Legal", 230, null, DOMSRC_DNB },
                { "tesla.com", "DUNS122", "DUNS122", null, 3131213L, "1123", 3, "Legal", 231, null, DOMSRC_DNB },
                // Case 4 : domain only entries
                { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes", 233, null,
                        DOMSRC_DNB },
                // Case 5 : duns only entries
                { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services", 234, null,
                        DOMSRC_DNB },
                // Case 6 : domains with missing ROOT_DUNS
                // MISSING_ROOT_DUNS reason type = no record entry found with
                // DUNS same as record's rootDuns
                // Here, rootDuns = DUNS900 and there is no entry with DUNS =
                // DUNS900 and hence reason type = MISSING_ROOT_DUNS
                { "netsuite.com", "DUNS890", "DUNS900", null, 32847L, "4547", 13, "Media", 236, null, DOMSRC_DNB }, };
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, data);
    }

    Object[][] expectedDataValues = new Object[][] { //
            // Domain, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER, REASON_TYPE, IS_NON_PROFITABLE SINGLE TREE : not cleaned up
            { "sbiGu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false" }, //
            { "karlDu.com", "DUNS24", "DU", 1, "SINGLE_TREE", "false" }, //
            { "netappGu.com", "DUNS28", "GU", 1, "SINGLE_TREE", "false" }, //
            { "mongodbGu.com", "DUNS17", "GU", 1, "SINGLE_TREE", "false" }, //
            { "vlocityGu.com", "DUNS77", "GU", 1, "SINGLE_TREE", "false" }, //
            { "rakutenGu.com", "DUNS82", "GU", 1, "SINGLE_TREE", "false" }, //
            { "craigsGu.com", "DUNS61", "GU", 1, "SINGLE_TREE", "false" }, //
            { "craigsDu.com", "DUNS62", "DU", 1, "SINGLE_TREE", "false" }, //
            // FRANCHISE, MULTIPLE_LARGE_COMPANY : not cleaned up
            { "sbiDuns1.com", null, null, 4, "FRANCHISE", "false" }, //
            { "craigslist.com", null, null, 2, "MULTIPLE_LARGE_COMPANY", "false" }, //
            // OTHER : not cleaned up
            { "tesla.com", null, null, 3, "OTHER", "false" }, //
            // Reasons : HIGHER_NUM_OF_LOC, HIGHER_SALES_VOLUME,
            // HIGHER_EMP_TOTAL := Cleaned up
            { "sbiDuns2.com", "DUNS10", "GU", 2, "HIGHER_NUM_OF_LOC", "false" }, //
            { "karlDuns2.com", "DUNS28", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "vlocityDuns2.com", "DUNS77", "GU", 2, "HIGHER_EMP_TOTAL", "false" }, //
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
        Assert.assertEquals(rowCount, expectedDataValues.length);
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }
}
