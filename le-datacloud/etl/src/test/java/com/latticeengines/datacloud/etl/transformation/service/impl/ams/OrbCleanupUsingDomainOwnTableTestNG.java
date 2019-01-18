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
import com.latticeengines.datacloud.dataflow.transformation.CleanupOrbSecSrcFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class OrbCleanupUsingDomainOwnTableTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(OrbCleanupUsingDomainOwnTableTestNG.class);

    private static final String DOM_OWNERSHIP_TABLE = "DomainOwnershipTable";
    private static final String ORB_SEC_CLEANED = "OrbSecCleaned";

    private final static String ROOT_DUNS = DomainOwnershipConfig.ROOT_DUNS;
    private final static String DUNS_TYPE = DomainOwnershipConfig.DUNS_TYPE;
    private final static String TREE_NUMBER = DomainOwnershipConfig.TREE_NUMBER;
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String IS_NON_PROFITABLE = "IS_NON_PROFITABLE";

    private GeneralSource domOwnTable = new GeneralSource(DOM_OWNERSHIP_TABLE);
    private GeneralSource orbSec = new GeneralSource("OrbCacheSeedSecondaryDomain");
    private GeneralSource source = new GeneralSource(ORB_SEC_CLEANED);
    private GeneralSource alexa = new GeneralSource("AlexaMostRecent");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareDomOwnTable();
        prepareOrbSeedSecondaryDom();
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
            configuration.setName("OrbCacheSeedRebuild");
            configuration.setVersion(targetVersion);

            // -----------------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> cleanupOrbSecSrc = new ArrayList<String>();
            cleanupOrbSecSrc.add(domOwnTable.getSourceName());
            cleanupOrbSecSrc.add(orbSec.getSourceName());
            cleanupOrbSecSrc.add(alexa.getSourceName());
            step1.setBaseSources(cleanupOrbSecSrc);
            step1.setTransformer(CleanupOrbSecSrcFlow.TRANSFORMER_NAME);
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
        return JsonUtils.serialize(conf);
    }

    private void prepareAlexaData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.ALEXA_ATTR_URL, String.class));
        schema.add(Pair.of(DataCloudConstants.ALEXA_ATTR_RANK, Integer.class));
        Object[][] data = new Object[][] { //
                { "paypal.com", 700 }, { "sbiGu.com", 32 }, { "sbiDu.com", 36 },
                { "karlDu.com", 326 }, { "netappGu.com", 24 }, { "amazonGu.com", 252 }, { "mongodbDu.com", 15 },
                { "mongodbGu.com", 89 }, { "regalGoodWill.com", 21 }, { "goodWillOrg.com", 62 },
                { "netappDuns1.com", 83 }, { "mongoDbDuns1.com", 11 }, { "worldwildlife.org", 87 },
                { "wordwildlifeGu.org", 666 }, { "socialorg.com", 55 }, { "velocity.com", 44 },
                { "karlDuns2.com", 101 }, { "netappDuns2.com", 102 }, { "unicef.org", 103 }, { "goodwill.com", 104 },
                { "sbiDuns2.com", 105 }, { "amazon.com", 106 }, { "sbiDuns1.com", 107 }, { "tesla.com", 108 },
                { "netappDu.com", 109 }, { "netsuite.com", 110 }, { "paypalHQ.com", 111 }, { "rubrik.com", 113 },
                { "lyft.com", 114 }, { "intuit.com", 115 }, { "macys.com", 116 }, { "netappDuns3.com", 117 },
                { "oldnavy.com", 118 }, { "oracle.com", 134 }, { "netapp.com", 23 } };
        uploadBaseSourceData(alexa.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareDomOwnTable() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN, String.class));
        schema.add(Pair.of(ROOT_DUNS, String.class));
        schema.add(Pair.of(DUNS_TYPE, String.class));
        schema.add(Pair.of(TREE_NUMBER, Integer.class));
        schema.add(Pair.of(REASON_TYPE, String.class));
        schema.add(Pair.of(IS_NON_PROFITABLE, String.class));
        Object[][] data = new Object[][] {
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
        uploadBaseSourceData(domOwnTable.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareOrbSeedSecondaryDom() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.ORBSEC_ATTR_SECDOM, String.class));
        schema.add(Pair.of(DataCloudConstants.ORBSEC_ATTR_PRIDOM, String.class));
        // Schema: SecondaryDomain, PrimaryDomain
        Object[][] data = new Object[][] {
                // PriRootDuns == null, SecRootDuns == null
                { "airbnb.com", "lyft.com" },
                // PriRootDuns != null, SecRootDuns != null, PriRootDuns != SecRootDuns
                { "sbiDuns2.com", "karlDuns2.com" },
                // PriRootDuns != null, SecRootDuns == null
                { "karlDuns2.com", "netappDuns1.com" }, { "karlDuns2.com", "oldnavy.com" },
                // PriRootDuns != null, SecRootDuns != null, PriRootDuns == SecRootDuns
                { "sbiGu.com", "sbiDu.com" },
                // PriRootDuns != null, SecRootDuns == null
                { "netappGu.com", "paypal.com" },
                // Different PriDomains with same SecDomain : retain based on which has higher alexa rank
                { "sap.com", "oracle.com" }, { "sap.com", "netapp.com" } };
        uploadBaseSourceData(orbSec.getSourceName(), baseSourceVersion, schema, data);
    }

    Object[][] orbSecSrcCleanedupValues = new Object[][] { //
            // Schema: PrimaryDomain, SecondaryDomain

            // PriRootDuns == null, SecRootDuns == null
            { "lyft.com", "airbnb.com" },
            // PriRootDuns != null, SecRootDuns != null, PriRootDuns == SecRootDuns
            { "sbiDu.com", "sbiGu.com" },
            // Different PriDomains with same SecDomain : retain based on which has higher alexa rank
            { "oracle.com", "sap.com"}
    };


    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = new HashMap<>();
        for (Object[] data : orbSecSrcCleanedupValues) {
            expectedData.put(String.valueOf(data[0]) + String.valueOf(data[1]), data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : " + record);
            System.out.println("record : " + record);
            String priDomain = String.valueOf(record.get(0));
            String secDomain = String.valueOf(record.get(1));
            Object[] expected = expectedData.get(priDomain + secDomain);
            Assert.assertTrue(isObjEquals(priDomain, expected[0]));
            Assert.assertTrue(isObjEquals(secDomain, expected[1]));
            expectedData.remove(priDomain + secDomain);
            rowCount++;
        }
        Assert.assertTrue(expectedData.size() == 0);
        Assert.assertEquals(rowCount, 3);
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

}
