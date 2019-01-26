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

    private GeneralSource domOwnTable = new GeneralSource(
            DomainOwnershipConfig.DOM_OWNERSHIP_TABLE);
    private GeneralSource orbSec = new GeneralSource("OrbCacheSeedSecondaryDomain");
    private GeneralSource source = new GeneralSource(DomainOwnershipConfig.ORB_SEC_CLEANED);
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
                { "paypal.com", 700 }, { "sbiDu.com", 36 },
                { "netappDuns1.com", 83 }, { "karlDuns2.com", 101 }, { "lyft.com", 114 },
                { "oldnavy.com", 118 }, { "oracle.com", 134 }, { "datos.io", null },
                { "netapp.com", 150 }, { "emc.com", null } };
        uploadBaseSourceData(alexa.getSourceName(), baseSourceVersion, schema, data);
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
         // Domain, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER, REASON_TYPE, IS_NON_PROFITABLE
            // PriRootDuns != null, SecRootDuns != null, PriRootDuns == SecRootDuns
            { "sbiGu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false" }, //
            { "sbiDu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false"}, //
            
            // PriRootDuns == null, SecRootDuns != null
            { "netappGu.com", "DUNS28", "GU", 1, "SINGLE_TREE", "false" }, //
            
            // PriRootDuns != null, SecRootDuns == null
            { "karlJr.com", null, null, 2, "MISSING_ROOT_DUNS", "false"}, //
            
            // PriRootDuns != null, SecRootDuns != null, PriRootDuns != SecRootDuns
            { "sbiDuns2.com", "DUNS10", "GU", 2, "HIGHER_NUM_OF_LOC", "false" }, //
            { "karlDuns2.com", "DUNS28", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
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
                { "tjmax.com", "sbiGu.com" },
                { "karlJr.com", "sbiDu.com" },
                // PriRootDuns == null, SecRootDuns != null
                { "karlDuns2.com", "netappDuns1.com" }, { "karlDuns2.com", "oldnavy.com" },
                { "netappGu.com", "paypal.com" },
                // PriRootDuns != null, SecRootDuns != null, PriRootDuns == SecRootDuns
                { "sbiGu.com", "sbiDu.com" },
                // Different PriDomains with same SecDomain : retain based on which has lower alexa rank
                // Alexa rank for oracle.com = 134 and netapp.com = 150. Here, oracle.com is retained
                { "sap.com", "oracle.com" }, { "sap.com", "netapp.com" },
                // PriDomains with null AlexaRank
                // AlexaRank = null is worse than any number i.e any number wins
                // over null as null is considered the lowest alexaRank
                // 0 is considered the highest alexaRank
                { "sap.com", "emc.com" }, { "data.com", "datos.io" },
                { "data.com", "salesforce.com" } };
        uploadBaseSourceData(orbSec.getSourceName(), baseSourceVersion, schema, data);
    }

    Object[][] orbSecSrcCleanedupDeterministicSet = new Object[][] { //
            // Schema: PrimaryDomain, SecondaryDomain

            // PriRootDuns == null, SecRootDuns == null
            { "lyft.com", "airbnb.com" },
            // PriRootDuns != null, SecRootDuns != null, PriRootDuns == SecRootDuns
            { "sbiDu.com", "sbiGu.com" },
            // PriRootDuns != null, SecRootDuns == null
            { "sbiGu.com", "tjmax.com"},
            { "sbiDu.com", "karlJr.com" },
            // Different PriDomains with same SecDomain : retain based on which has lower alexa rank
            { "oracle.com", "sap.com" },
    };

    Object[][] orbSecSrcCleanedupNotDeterministicSet = new Object[][] { //
            // Schema: PrimaryDomain, SecondaryDomain

            // Diff PriDomains with same Sec Domain : with same alexa rank = so
            // anyone will be retained
            { "salesforce.com", "data.com" }, { "datos.io", "data.com" } };


    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedDeterministicSet = new HashMap<>();
        for (Object[] data : orbSecSrcCleanedupDeterministicSet) {
            expectedDeterministicSet.put(String.valueOf(data[0]) + String.valueOf(data[1]), data);
        }
        Map<String, Object[]> expectedNonDeterministicSet = new HashMap<>();
        for (Object[] data : orbSecSrcCleanedupNotDeterministicSet) {
            expectedNonDeterministicSet.put(String.valueOf(data[0]) + String.valueOf(data[1]),
                    data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : " + record);
            String priDomain = String.valueOf(record.get(0));
            String secDomain = String.valueOf(record.get(1));
            Object[] expected = expectedDeterministicSet.get(priDomain + secDomain);
            if (expected == null) {
                expected = expectedNonDeterministicSet.get(priDomain + secDomain);
            }
            Assert.assertTrue(isObjEquals(priDomain, expected[0]));
            Assert.assertTrue(isObjEquals(secDomain, expected[1]));
            expectedDeterministicSet.remove(priDomain + secDomain);
            rowCount++;
        }
        Assert.assertTrue(expectedDeterministicSet.size() == 0);
        Assert.assertEquals(rowCount, 6);
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

}
