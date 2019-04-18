package com.latticeengines.datacloud.etl.transformation.service.impl.ams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.ams.AMSeedCleanByDomainOwner;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.ams.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

public class AMSeedCleanByDomainOwnerTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DomainOwnershipRebuildTestNG.class);

    private static final String DOMSRC_DNB = DataCloudConstants.DOMSRC_DNB;
    private static final String DOMSRC_ORB = DataCloudConstants.DOMSRC_ORB;

    private GeneralSource domOwnTable = new GeneralSource(
            DomainOwnershipConfig.DOM_OWNERSHIP_TABLE);
    private GeneralSource orbSecClean = new GeneralSource(DomainOwnershipConfig.ORB_SEC_CLEANED);
    private GeneralSource amsClean = new GeneralSource(DomainOwnershipConfig.AMS_CLEANED);
    private GeneralSource ams = new GeneralSource("AccountMasterSeed");
    private GeneralSource alexa = new GeneralSource("AlexaMostRecent");
    private GeneralSource source = amsClean;

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareDomOwnTable();
        prepareAmSeed();
        prepareOrbSeedSecCleaned();
        prepareAlexaData();
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
            configuration.setName("AmSeedCleanup");
            configuration.setVersion(targetVersion);

            // -----------------
            TransformationStepConfig step0 = new TransformationStepConfig();
            List<String> cleanupAmSeedSrc = new ArrayList<String>();
            cleanupAmSeedSrc.add(domOwnTable.getSourceName());
            cleanupAmSeedSrc.add(ams.getSourceName());
            cleanupAmSeedSrc.add(orbSecClean.getSourceName());
            cleanupAmSeedSrc.add(alexa.getSourceName());
            step0.setBaseSources(cleanupAmSeedSrc);
            step0.setTransformer(AMSeedCleanByDomainOwner.TRANSFORMER_NAME);
            String confParamStr1 = getDomOwnershipTableConfig();
            step0.setConfiguration(confParamStr1);
            step0.setTargetSource(amsClean.getSourceName());

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step0);

            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDomOwnershipTableConfig() throws JsonProcessingException {
        DomainOwnershipConfig conf = new DomainOwnershipConfig();
        return JsonUtils.serialize(conf);
    }

    private void prepareDomOwnTable() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN, String.class));
        schema.add(Pair.of(DomainOwnershipConfig.ROOT_DUNS, String.class));
        schema.add(Pair.of(DomainOwnershipConfig.DUNS_TYPE, String.class));
        schema.add(Pair.of(DomainOwnershipConfig.TREE_NUMBER, Integer.class));
        schema.add(Pair.of(DomainOwnershipConfig.REASON_TYPE, String.class));
        schema.add(Pair.of(DomainOwnershipConfig.IS_NON_PROFITABLE, String.class));
        Object[][] data = new Object[][] {
                // Domain, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER, REASON_TYPE,
                // IS_NON_PROFITABLE SINGLE TREE : not cleaned up
                { "sbiGu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false" }, //
                // FRANCHISE, MULTIPLE_LARGE_COMPANY : not cleaned up
                { "sbiDuns1.com", null, null, 4, "FRANCHISE", "false" }, //
                { "craigslist.com", null, null, 2, "MULTIPLE_LARGE_COMPANY", "false" }, //
                // OTHER : not cleaned up
                { "tesla.com", null, null, 3, "OTHER", "false" }, //
                // Reasons : HIGHER_EMP_TOTAL, HIGHER_NUM_OF_LOC,
                // HIGHER_SALES_VOLUME := Cleaned up
                { "sbiDuns2.com", "DUNS10", "GU", 2, "HIGHER_NUM_OF_LOC", "false" }, //
                { "karlDuns2.com", "DUNS28", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
                { "vlocityDuns2.com", "DUNS77", "GU", 2, "HIGHER_EMP_TOTAL", "false" }, //
                // Missing root DUNS entry case (in single or multiple trees)
                // rootDuns = DUNS900
                { "netsuite.com", null, null, 1, "MISSING_ROOT_DUNS", "false" }, //
        };
        uploadBaseSourceData(domOwnTable.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareAlexaData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.ALEXA_ATTR_URL, String.class));
        schema.add(Pair.of(DataCloudConstants.ALEXA_ATTR_RANK, Integer.class));
        Object[][] data = new Object[][] { //
                { "paypal.com", 700 }, { "sbiGu.com", 32 }, { "netappDuns1.com", 83 },
                { "karlDuns2.com", 101 }, { "sbiDuns2.com", 105 }, { "sbiDuns1.com", 107 },
                { "tesla.com", 108 }, { "netappDu.com", 109 }, { "netsuite.com", 110 },
                { "oracle.com", 134 }, { "music.com", 11 } };
        uploadBaseSourceData(alexa.getSourceName(), baseSourceVersion, schema, data);
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
            { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production", 200, null, DOMSRC_DNB },
                // Case 2 : domains present in OwnershipTable : rootDuns match
                // sbiDuns2.com domain record with DUNS = DUNS10 wins due to
                // reason = HIGHER_NUM_OF_LOC(same salesVolume and
                // totalEmployees)
            { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal", 216, null, DOMSRC_DNB },
                // vlocityDuns2.com domain record with DUNS = DUNS78 wins due to
                // reason = HIGHER_EMP_TOTAL(same salesVolume)
            { "vlocityDuns2.com", "DUNS78", "DUNS77", "DUNS79", 40002499L, "4552", 2, "Media", 342, null, DOMSRC_DNB },
                // domains present in OwnershipTable : rootDuns doesn't
                // match = these are the entries that will get cleaned up as same domain
                // in other tree (other root duns) got selected as provided above
                // karlDuns2.com domain record with DUNS = DUNS24 loses due to
                // lower sales volume
            { "karlDuns2.com", "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research", 218, null, DOMSRC_DNB },
                // sbiDuns2.com domain record with DUNS = DUNS01 loses due to
                // lower num of locations(same salesVolume and totalEmployees)
            { "sbiDuns2.com", "DUNS01", null, "DUNS01", 21100024L, "50000", null, null, 223, null, DOMSRC_DNB },
                // vlocityDuns2.com domain record with DUNS = DUNS82 loses due
                // to lower total employees(same root duns salesVolume)
            { "vlocityDuns2.com", "DUNS88", "DUNS82", "DUNS80", 70002499L, "6552", 4, "Legal", 123, null, DOMSRC_DNB },
                // Case 3 : domains present in OwnershipTable with reasons
                // multiple large company, franchise, other
                // Present in multiple trees causing reasonType = MULTIPLE_LARGE_COMPANY
            { "craigslist.com", "DUNS07", "DUNS61", null, 6660405L, "23552", 2, "Legal", 31,
                null, DOMSRC_DNB },
            { "craigslist.com", "DUNS03", null, "DUNS62", 5020405L, "2123", 2, "Media", 142,
                null, DOMSRC_DNB },
                // ReasonType = OTHER
                // Added reason_type = 'OTHER' for domains present in multiple trees having firmographics same in both trees.
                // So, can't choose the tree, so will update ROOT_DUNS = null and DUNS_TYPE = null for such entries.
                // Such entries will not be cleaned up
            { "tesla.com", "DUNS111", "DUNS111", "DUNS110", 3131213L, "1123", 3, "Legal", 229, null, DOMSRC_DNB },
            { "tesla.com", "DUNS121", "DUNS121", "DUNS120", 3131213L, "1123", 3, "Legal", 230, null, DOMSRC_DNB },
            { "tesla.com", "DUNS122", "DUNS122", null, 3131213L, "1123", 3, "Legal", 231, null, DOMSRC_DNB },
                // Case 4 : domain only entries
            { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes", 233, null, DOMSRC_DNB },
            // Case 5 : duns only entries
            { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services",
                        234, null, DOMSRC_DNB },
            // Case 6 : domains with missing ROOT_DUNS
                // MISSING_ROOT_DUNS reason type = no record entry found with
                // DUNS same as record's rootDuns.
                // Here, rootDuns = DUNS900 and
                // there is no entry with DUNS = DUNS900 and hence reason type =
                // MISSING_ROOT_DUNS
            { "netsuite.com", "DUNS890", "DUNS900", null, 32847L, "4547", 13, "Media", 236,
                    null, DOMSRC_DNB },
            // Case 7 :
            // sbiDuns1.com domain replaced with domain paypal.com. Update corresponding alexaRank.
            { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services", 225, null, DOMSRC_DNB },
            { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1,"Manufacturing - Semiconductors", 226, null, DOMSRC_DNB },
            { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology", 227, null, DOMSRC_DNB },
            { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production", 228, null, DOMSRC_DNB },
            // Case 8 :
            // Domain only entry : karlDuns2.com domain record got replaced with domain netappDuns1.com. Update corresponding alexaRank.
            { "karlDuns2.com", null, "DUNS28", null, 304500L, "2200", 1, "Media", 215, null,
                    DOMSRC_DNB },
            // Case 9 :
            // saavn.com exists in OrbSec.SecondaryDomain, then this domain is replaced by OrbSec.PrimaryDomain.
            // Primary domain already exists in AMSeed. The final result should not have duplicate domain + duns entries.
            { "music.com", "DUNS37", null, null, 24178781L, "2343", 2, "Media", 213, null,
                        DOMSRC_DNB },
            { "saavn.com", "DUNS37", "DUNS39", "DUNS90", 2812732L, "1313", 2, "Legal", 131, null, DOMSRC_DNB}
        };
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareOrbSeedSecCleaned() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.ORBSEC_ATTR_PRIDOM, String.class));
        schema.add(Pair.of(DataCloudConstants.ORBSEC_ATTR_SECDOM, String.class));
        Object[][] data = new Object[][] {
                // Schema: PrimaryDomain, SecondaryDomain

                // Add corresponding Primary domain from OrbSecSrc to AmSeed
                // with values
                // similar to that of secondary domain as the corresponding
                // Secondary domain is present in amSeed
                { "netappDuns1.com", "karlDuns2.com" },
                { "paypal.com", "sbiDuns1.com" },

                // saavn.com exists in OrbSec.SecondaryDomain, then this domain is replaced by OrbSec.PrimaryDomain.
                // Primary domain already exists in AMSeed. The final result should not have duplicate domain + duns entries.
                { "music.com", "saavn.com" },

                // primary domain not added as corresponding secondary domain
                // not present in amSeed
                { "oracle.com", "sap.com" },
        };
        uploadBaseSourceData(orbSecClean.getSourceName(), baseSourceVersion, schema, data);
    }

    Object[][] amSeedCleanedUpValues = new Object[][] { //
            // Domain, DUNS, GU, DU, SalesVolume, EmpTotal, NumOfLoc, PrimInd,
            // AlexaRank, LE_OperationLogs, LE_IS_PRIMARY_DOMAIN, DomainSource
            // Case 1 : Domains present in domain ownership table with SINGLE_TREE type : result = not cleaned up
            { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production", 200, null, null, DOMSRC_DNB },

            // Case 2 : Domains present in OwnershipTable : rootDuns match = not cleaned up
            { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal", 216,
                    null, null, DOMSRC_DNB },
            { "vlocityDuns2.com", "DUNS78", "DUNS77", "DUNS79", 40002499L, "4552", 2, "Media", 342,
                    null, null, DOMSRC_DNB },
            // domains present in OwnershipTable : rootDuns doesnt match = cleaned up
            { null, "DUNS01", null, "DUNS01", 21100024L, "50000", null, null, null,
                    "[Step=AMSeedCleanByDomainOwner,Code=CLEAN_DOM_BY_OWNER,Log=Owned by duns DUNS10 with reason HIGHER_NUM_OF_LOC]",
                    "N", DOMSRC_DNB },
            { null, "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research", null,
                    "[Step=AMSeedCleanByDomainOwner,Code=CLEAN_DOM_BY_OWNER,Log=Owned by duns DUNS28 with reason HIGHER_SALES_VOLUME]",
                    "N", DOMSRC_DNB },
            { null, "DUNS88", "DUNS82", "DUNS80", 70002499L, "6552", 4, "Legal", null,
                    "[Step=AMSeedCleanByDomainOwner,Code=CLEAN_DOM_BY_OWNER,Log=Owned by duns DUNS77 with reason HIGHER_EMP_TOTAL]",
                    "N", DOMSRC_DNB },
            // Case 3 : domains present in OwnershipTable with reasons multiple large company, franchise, other = not cleaned up
            { "tesla.com", "DUNS111", "DUNS111", "DUNS110", 3131213L, "1123", 3, "Legal", 229, null, null, DOMSRC_DNB },
            { "tesla.com", "DUNS121", "DUNS121", "DUNS120", 3131213L, "1123", 3, "Legal", 230, null, null, DOMSRC_DNB },
            { "tesla.com", "DUNS122", "DUNS122", null, 3131213L, "1123", 3, "Legal", 231, null, null, DOMSRC_DNB },
            { "craigslist.com", "DUNS07", "DUNS61", null, 6660405L, "23552", 2, "Legal", 31, null, null, DOMSRC_DNB },
            { "craigslist.com", "DUNS03", null, "DUNS62", 5020405L, "2123", 2, "Media", 142, null, null, DOMSRC_DNB },
            // Case 4 : domain only entries = not cleaned up
            { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes", 233, null, null, DOMSRC_DNB },
            // Case 5 : duns only entries = not cleaned up
            { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services", 234,
                    null, null, DOMSRC_DNB },
            // Case 6 : domains with missing ROOT_DUNS
            { "netsuite.com", "DUNS890", "DUNS900", null, 32847L, "4547", 13, "Media", 236, null,
                    null, DOMSRC_DNB },
            // Case 7 :

            // sbiDuns1.com domain replaced with domain paypal.com. Update corresponding alexaRank.
            { "paypal.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services",
                    700,"[Step=AMSeedCleanByDomainOwner,Code=SECDOM_TO_PRI,Log=sbiDuns1.com is orb sec domain]",
                    null, DOMSRC_ORB },
            { "paypal.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1, "Manufacturing - Semiconductors", 700,
                        "[Step=AMSeedCleanByDomainOwner,Code=SECDOM_TO_PRI,Log=sbiDuns1.com is orb sec domain]", null,
                    DOMSRC_ORB },
            { "paypal.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production", 700,
                        "[Step=AMSeedCleanByDomainOwner,Code=SECDOM_TO_PRI,Log=sbiDuns1.com is orb sec domain]", null,
                    DOMSRC_ORB },
            { "paypal.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology", 700,
                        "[Step=AMSeedCleanByDomainOwner,Code=SECDOM_TO_PRI,Log=sbiDuns1.com is orb sec domain]", null,
                        DOMSRC_ORB },
            // Case 8 :
            // Domain only entry : karlDuns2.com domain record got replaced with domain netappDuns1.com. Update corresponding alexaRank.
            { "netappDuns1.com", null, "DUNS28", null, 304500L, "2200", 1, "Media", 83,
                    "[Step=AMSeedCleanByDomainOwner,Code=SECDOM_TO_PRI,Log=karlDuns2.com is orb sec domain]", null,
                    DOMSRC_ORB },
            // Case 9 :
            // Domains present in domain ownership table with SINGLE_TREE type :
            // result = not cleaned up
            // amSeed domain exists in OrbSec.SecondaryDomain, then this domain
            // is replaced by OrbSec.PrimaryDomain.
            // Primary domain already exists in AMSeed. The final result should
            // not have duplicate domain + duns entries.
            { "music.com", "DUNS37", null, null, 24178781L, "2343", 2, "Media", 213, null,
                    null, DOMSRC_DNB }, };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> amSeedExpectedValues = new HashMap<>();
        for (Object[] data : amSeedCleanedUpValues) {
            amSeedExpectedValues.put(String.valueOf(data[0]) + String.valueOf(data[1]),
                    data);
        }
        String[] expectedValueOrder = { //
                DataCloudConstants.AMS_ATTR_DOMAIN, //
                DataCloudConstants.AMS_ATTR_DUNS, //
                DataCloudConstants.ATTR_GU_DUNS, //
                DataCloudConstants.ATTR_DU_DUNS, //
                DataCloudConstants.ATTR_SALES_VOL_US, //
                DataCloudConstants.ATTR_EMPLOYEE_TOTAL, //
                DataCloudConstants.ATTR_LE_NUMBER_OF_LOCATIONS, //
                DataCloudConstants.AMS_ATTR_PRIMARY_INDUSTRY, //
                DataCloudConstants.ATTR_ALEXA_RANK, //
                OperationLogUtils.DEFAULT_FIELD_NAME, //
                DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN, //
                DataCloudConstants.AMS_ATTR_DOMAIN_SOURCE };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : " + record);
            String domain = String.valueOf(record.get("Domain"));
            String duns = String.valueOf(record.get("DUNS"));
            Object[] expectedVal = amSeedExpectedValues.get(domain + duns);
            for (int i = 0; i < expectedValueOrder.length; i++) {
                Assert.assertTrue(isObjEquals(record.get(expectedValueOrder[i]), expectedVal[i]));
            }
            amSeedExpectedValues.remove(domain + duns);
            rowCount++;
        }
        Assert.assertTrue(amSeedExpectedValues.size() == 0);
        Assert.assertEquals(rowCount, amSeedCleanedUpValues.length);
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }
}
