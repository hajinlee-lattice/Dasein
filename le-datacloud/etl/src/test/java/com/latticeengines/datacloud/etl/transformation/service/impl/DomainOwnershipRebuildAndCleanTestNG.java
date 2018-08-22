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
import com.latticeengines.datacloud.dataflow.transformation.CleanupAmSeedSrcFlow;
import com.latticeengines.datacloud.dataflow.transformation.CleanupOrbSecSrcFlow;
import com.latticeengines.datacloud.dataflow.transformation.DomainOwnershipRebuildFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DomainOwnershipConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DomainOwnershipRebuildAndCleanTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(DomainOwnershipRebuildAndCleanTestNG.class);

    private static final String DOM_OWNERSHIP_TABLE = "DomainOwnershipTable";
    private static final String ORB_SEC_CLEANED = "OrbSecCleaned";
    private static final String AMS_CLEANED = "AmsCleaned";
    GeneralSource domOwnTable = new GeneralSource(DOM_OWNERSHIP_TABLE);
    GeneralSource orbSecClean = new GeneralSource(ORB_SEC_CLEANED);
    GeneralSource amsClean = new GeneralSource(AMS_CLEANED);
    GeneralSource ams = new GeneralSource("AccountMasterSeed");
    GeneralSource orbSec = new GeneralSource("OrbCacheSeedSecondaryDomain");
    GeneralSource alexa = new GeneralSource("AlexaMostRecent");
    GeneralSource source = amsClean;

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareAmSeed();
        prepareOrbSeedSecondaryDom();
        prepareAlexaData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(domOwnTable, targetVersion);
        confirmIntermediateSource(orbSecClean, targetVersion);
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
            configuration.setName("DomainOwnershipRebuild");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(ams.getSourceName());
            baseSources.add(orbSec.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(DomainOwnershipRebuildFlow.TRANSFORMER_NAME);
            String confParamStr1 = getDomOwnershipTableConfig();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(domOwnTable.getSourceName());

            // -----------------
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> cleanupOrbSecSrcStep = new ArrayList<Integer>();
            List<String> cleanupOrbSecSrc = new ArrayList<String>();
            cleanupOrbSecSrcStep.add(0);
            cleanupOrbSecSrc.add(orbSec.getSourceName());
            step2.setInputSteps(cleanupOrbSecSrcStep);
            step2.setBaseSources(cleanupOrbSecSrc);
            step2.setTransformer(CleanupOrbSecSrcFlow.TRANSFORMER_NAME);
            step2.setConfiguration(confParamStr1);
            step2.setTargetSource(orbSecClean.getSourceName());

            // -----------------
            TransformationStepConfig step3 = new TransformationStepConfig();
            List<String> cleanupAmSeedSrc = new ArrayList<String>();
            List<Integer> cleanupAmSeedStep = new ArrayList<Integer>();
            cleanupAmSeedStep.add(0);
            cleanupAmSeedSrc.add(ams.getSourceName());
            cleanupAmSeedSrc.add(orbSecClean.getSourceName());
            cleanupAmSeedSrc.add(alexa.getSourceName());
            step3.setInputSteps(cleanupAmSeedStep);
            step3.setBaseSources(cleanupAmSeedSrc);
            step3.setTransformer(CleanupAmSeedSrcFlow.TRANSFORMER_NAME);
            step3.setConfiguration(confParamStr1);
            step3.setTargetSource(amsClean.getSourceName());

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            steps.add(step3);

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

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    private void prepareAlexaData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("URL", String.class));
        schema.add(Pair.of("Rank", Integer.class));
        Object[][] data = new Object[][] { { "paypal.com", 700 }, { "rubrik.com", 701 }, { "sbiGu.com", 32 },
                { "sbiDu.com", 36 }, { "karlDu.com", 326 }, { "netappGu.com", 24 }, { "amazonGu.com", 252 },
                { "mongodbDu.com", 15 }, { "mongodbGu.com", 89 }, { "regalGoodWill.com", 21 },
                { "goodWillOrg.com", 62 }, { "netappDuns1.com", 83 }, { "mongoDbDuns1.com", 11 },
                { "worldwildlife.org", 87 }, { "wordwildlifeGu.org", 666 }, { "socialorg.com", 55 },
                { "velocity.com", 44 }, { "karlDuns2.com", 101 }, { "netappDuns2.com", 102 }, { "unicef.org", 103 },
                { "goodwill.com", 104 }, { "sbiDuns2.com", 105 }, { "amazon.com", 106 }, { "sbiDuns1.com", 107 },
                { "tesla.com", 108 }, { "netappDu.com", 109 }, { "netsuite.com", 110 }, { "paypalHQ.com", 111 },
                { "rubrik.com", 113 }, { "lyft.com", 114 }, { "intuit.com", 115 },
                { "macys.com", 116 }, { "netappDuns3.com", 117 }, { "oldnavy.com", 118 } };
        uploadBaseSourceData(alexa.getSourceName(), baseSourceVersion, schema, data);
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
        schema.add(Pair.of("AlexaRank", Integer.class));
        Object[][] data = new Object[][] {
                // domains not present in domainOwnershipTable
                { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production", 200 },
                { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services", 201 },
                { "karlDu.com", "DUNS24", null, "DUNS24", 21100024L, "50000", 3, "Accounting", 202 },
                { "netappGu.com", "DUNS28", "DUNS28", null, 2250000262L, "55000", 20, "Passenger Car Leasing", 203 },
                { "amazonGu.com", "DUNS36", "DUNS36", null, 3250000242L, "11000", 2, "Energy", 204 },
                { "mongodbDu.com", "DUNS18", "DUNS17", "DUNS18", 510002421L, "22009", 9, null, 205 },
                { "mongodbGu.com", "DUNS17", "DUNS17", "DUNS18", 2250000242L, "67009", 34, "Legal", 206 },
                { "regalGoodWill.com", "DUNS55", "DUNS55", null, 9728329L, "2230", 11, "Media", 207 },
                { "goodWillOrg.com", "DUNS59", "DUNS59", null, 82329840L, "2413", 10, "Media", 208 },
                { "netappDuns1.com", "DUNS31", "DUNS28", null, 30450010L, "10000", 3, "Junior Colleges", 209 },
                { "mongoDbDuns1.com", "DUNS21", "DUNS17", "DUNS18", 30450010L, "10000", 1, "Wholesale", 210 },
                { "worldwildlife.org", "DUNS06", "DUNS39", null, 204500L, "1500", 1, "Government", 211 },
                { "wordwildlifeGu.org", "DUNS39", "DUNS39", "DUNS38", 304500L, "3700", 3, "Education", 212 },
                { "socialorg.com", "DUNS54", null, null, 94500L, "98924", 2, "Education", 213 },
                { "velocity.com", "DUNS96", "DUNS8", null, 131314L, "232", 1, "Media", 214 },
                // domains present in OwnershipTable : rootDuns match
                { "karlDuns2.com", "DUNS34", "DUNS28", null, 304500L, "2200", 1, "Media", 215 },
                { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal", 216 },
                // domains present in OwnershipTable : rootDuns doesnt match
                { "karlDuns1.com", "DUNS97", null, "DUNS24", 30191910L, "1001", 1, "Accounting", 217 },
                { "karlDuns2.com", "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research", 218 },
                { "netappDuns2.com", "DUNS33", null, null, 30450010L, "8000", 3, "Biotechnology", 219 },
                { "unicef.org", "DUNS22", null, null, 104500L, "3700", 2, "Non-profit", 220 },
                { "goodwill.com", "DUNS53", "DUNS55", null, 8502491L, "1232", 2, "Media", 221 },
                { "goodwill.com", "DUNS79", null, "DUNS59", 9502492L, "2714", 2, "Media", 222 },
                { "sbiDuns2.com", "DUNS01", null, "DUNS01", 21100024L, "50000", null, null, 223 },
                // domains present in OwnershipTable with reasons multiple large
                // company, franchise, other
                { "amazon.com", "DUNS37", "DUNS36", null, null, "2200", 1, "Media", 224 },
                { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services", 225 },
                { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1,
                        "Manufacturing - Semiconductors", 226 },
                { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology", 227 },
                { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production", 228 },
                { "tesla.com", "DUNS111", "DUNS111", "DUNS110", 3131213L, "1123", 3, "Legal", 229 },
                { "tesla.com", "DUNS121", "DUNS121", "DUNS120", 3131213L, "1123", 3, "Legal", 230 },
                { "tesla.com", "DUNS122", "DUNS122", null, 3131213L, "1123", 3, "Legal", 231 },
                // domain only entries
                { "amazon.com", null, "DUNS17", "DUNS18", 100002421L, null, 1, "Manufacturing - Semiconductors", 232 },
                { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes", 233 },
                // duns only entries
                { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services", 234 },
                { null, "DUNS69", null, "DUNS69", 231131L, "1313", 2, "Non-profit", 235 },
                // orb entry gets selected
                { "netsuite.com", "DUNS890", "DUNS900", null, 32847L, "4547", 13, "Media", 236 },
                { "paypalHQ.com", "DUNS891", "DUNS891", null, 23284781L, "447", 3, "Media", 237 },
                { "paypal.com", "DUNS75", "DUNS75", null, 37875812L, "2425", 341, "Legal", 238 },
                { "paypal.com", "DUNS76", "DUNS891", null, 3787581L, "2425", 341, "Legal", 239 },
                { "rubrik.com", "DUNS70", "DUNS75", null, 128312L, "2133", 22, "Legal", 240 },
                { "rubrik.com", "DUNS89", "DUNS900", null, 126612L, "4547", 13, "Media", 241 },
        };
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareOrbSeedSecondaryDom() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("SecondaryDomain", String.class));
        schema.add(Pair.of("PrimaryDomain", String.class));
        Object[][] data = new Object[][] {
                // PriRootDuns == null, SecRootDuns == null
                { "airbnb.com", "lyft.com" },
                { "apple.com", "uber.com" },
                { "datos.com", "intuit.com" },
                { "target.com", "macys.com" },
                { "amazon.com", "netappDuns1.com" },
                { "amazon.com", "mongoDbDuns1.com" },
                { "craigslist.com", "netappDuns1.com" },
                { "dell.com", "netappDuns3.com" },
                // PriRootDuns != null, SecRootDuns != null, PriRootDuns != SecRootDuns
                { "karlDuns1.com", "netappDuns2.com" },
                { "sbiDuns2.com", "karlDuns1.com" },
                // PriRootDuns == null, SecRootDuns != null
                { "karlDuns2.com", "netappDuns1.com" },
                { "karlDuns2.com", "oldnavy.com" },
                { "netappDuns2.com", "mongoDbDuns1.com" },
                { "unicef.org", "worldwildlife.org" },
                { "goodwill.com", "socialorg.com" },
                // PriRootDuns != null, SecRootDuns != null, PriRootDuns == SecRootDuns
                { "rubrik.com", "paypal.com" },
                // PriRootDuns != null, SecRootDuns == null
                { "sbiDuns1.com", "paypal.com" },
        };
        uploadBaseSourceData(orbSec.getSourceName(), baseSourceVersion, schema, data);
    }

    Object[][] expectedDataValues = new Object[][] { //
            // Domain, ROOT_DUNS, DUNS_TYPE, TREE_NUMBER, REASON_TYPE, IS_NON_PROFITABLE
            { "karlDuns1.com", "DUNS33", "DUNS", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "sbiDuns2.com", "DUNS10", "GU", 3, "HIGHER_NUM_OF_LOC", "false" }, //
            { "karlDuns2.com", "DUNS28", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "amazon.com", null, null, 3, "MULTIPLE_LARGE_COMPANY", "false" }, //
            { "netappDuns2.com", "DUNS17", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "sbiDuns1.com", null, null, 6, "FRANCHISE", "false" }, //
            { "unicef.org", "DUNS39", "GU", 2, "HIGHER_NUM_OF_LOC", "true" }, //
            { "goodwill.com", "DUNS54", "DUNS", 3, "HIGHER_EMP_TOTAL", "true" }, //
            { "tesla.com", null, null, 3, "OTHER", "false" }, //
            { "paypal.com", "DUNS75", "GU", 2, "HIGHER_SALES_VOLUME", "false" }, //
            { "netappDuns1.com", "DUNS28", "GU", 1, "SINGLE_TREE", "false" }, //
            { "goodWillOrg.com", "DUNS59", "GU", 1, "SINGLE_TREE", "false" }, //
            { "netappGu.com", "DUNS28", "GU", 1, "SINGLE_TREE", "false" }, //
            { "paypalHQ.com", "DUNS891", "GU", 1, "SINGLE_TREE", "false" }, //
            { "mongoDbDuns1.com", "DUNS17", "GU", 1, "SINGLE_TREE", "false" }, //
            { "wordwildlifeGu.org", "DUNS39", "GU", 1, "SINGLE_TREE", "true" }, //
            { "worldwildlife.org", "DUNS39", "GU", 1, "SINGLE_TREE", "true" }, //
            { "amazonGu.com", "DUNS36", "GU", 1, "SINGLE_TREE", "false" }, //
            { "regalGoodWill.com", "DUNS55", "GU", 1, "SINGLE_TREE", "false" }, //
            { "karlDu.com", "DUNS24", "DU", 1, "SINGLE_TREE", "false" }, //
            { "craigslist.com", "DUNS28", "GU", 1, "SINGLE_TREE", "false" }, //
            { "sbiGu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false" }, //
            { "mongodbDu.com", "DUNS17", "GU", 1, "SINGLE_TREE", "false" }, //
            { "mongodbGu.com", "DUNS17", "GU", 1, "SINGLE_TREE", "false" }, //
            { "sbiDu.com", "DUNS10", "GU", 1, "SINGLE_TREE", "false" }, //
            { "socialorg.com", "DUNS54", "DUNS", 1, "SINGLE_TREE", "true" }, //
            // missing root DUNS entry case (in single or multiple trees)
            // rootDuns = DUNS900
            { "netsuite.com", null, null, 1, "MISSING_ROOT_DUNS", "false" }, //
            // rootDuns = 900
            { "rubrik.com", null, null, 3, "MISSING_ROOT_DUNS", "false" }, //
            // rootDuns = DUNS8
            { "velocity.com", null, null, 1, "MISSING_ROOT_DUNS", "false" }, //
    };

    Object[][] amSeedCleanedUpValues = new Object[][] { //
            // Domain, DUNS, GU, DU, SalesVolume, EmpTotal, NumOfLoc, PrimInd, AlexaRank
            // domains not present in OwnershipTable : result = domain not
            // cleaned up
            { "sbiGu.com", "DUNS10", "DUNS10", "DUNS11", 21100024L, "50000", 60, "Food Production", 200 },
            { "sbiDu.com", "DUNS11", "DUNS10", "DUNS11", 250000242L, "20000", 30, "Consumer Services", 201 },
            { "karlDu.com", "DUNS24", null, "DUNS24", 21100024L, "50000", 3, "Accounting", 202 },
            { "netappGu.com", "DUNS28", "DUNS28", null, 2250000262L, "55000", 20, "Passenger Car Leasing", 203 },
            { "amazonGu.com", "DUNS36", "DUNS36", null, 3250000242L, "11000", 2, "Energy", 204 },
            { "mongodbDu.com", "DUNS18", "DUNS17", "DUNS18", 510002421L, "22009", 9, null, 205 },
            { "mongodbGu.com", "DUNS17", "DUNS17", "DUNS18", 2250000242L, "67009", 34, "Legal", 206 },
            { "regalGoodWill.com", "DUNS55", "DUNS55", null, 9728329L, "2230", 11, "Media", 207 },
            { "goodWillOrg.com", "DUNS59", "DUNS59", null, 82329840L, "2413", 10, "Media", 208 },
            { "netappDuns1.com", "DUNS31", "DUNS28", null, 30450010L, "10000", 3, "Junior Colleges", 209 },
            { "mongoDbDuns1.com", "DUNS21", "DUNS17", "DUNS18", 30450010L, "10000", 1, "Wholesale", 210 },
            { "worldwildlife.org", "DUNS06", "DUNS39", null, 204500L, "1500", 1, "Government", 211 },
            { "wordwildlifeGu.org", "DUNS39", "DUNS39", "DUNS38", 304500L, "3700", 3, "Education", 212 },
            { "socialorg.com", "DUNS54", null, null, 94500L, "98924", 2, "Education", 213 },
            { "velocity.com", "DUNS96", "DUNS8", null, 131314L, "232", 1, "Media", 214 },
            // domains present in OwnershipTable (rootDuns match) : result = domain not cleaned up
            { "netappDuns1.com", "DUNS34", "DUNS28", null, 304500L, "2200", 1, "Media", 83 }, // example domain replaced by amSeedCleanup
            // domains present in OwnershipTable (rootDuns doesn't match) : result = domain cleaned up
            { null, "DUNS01", null, "DUNS01", 21100024L, "50000", null, null, 223 },
            { null, "DUNS97", null, "DUNS24", 30191910L, "1001", 1, "Accounting", 217 },
            { null, "DUNS27", null, "DUNS24", 30450010L, "220", 2, "Research", 218 },
            { null, "DUNS33", null, null, 30450010L, "8000", 3, "Biotechnology", 219 },
            { null, "DUNS22", null, null, 104500L, "3700", 2, "Non-profit", 220 },
            { null, "DUNS53", "DUNS55", null, 8502491L, "1232", 2, "Media", 221 },
            { null, "DUNS79", null, "DUNS59", 9502492L, "2714", 2, "Media", 222 },
            // domains present in OwnershipTable with reasons multiple large
            // company, franchise : result = not cleaned up
            { "amazon.com", "DUNS37", "DUNS36", null, null, "2200", 1, "Media", 224 },
            { "sbiDuns1.com", "DUNS13", "DUNS10", "DUNS11", 50000242L, "7000", 2, "Consumer Services", 225 },
            { "sbiDuns1.com", "DUNS20", "DUNS17", "DUNS18", 200002421L, "11000", 1, "Manufacturing - Semiconductors",
                    226 },
            { "sbiDuns1.com", "DUNS66", "DUNS28", null, 99991910L, "10801", 2, "Biotechnology", 227 },
            { "sbiDuns1.com", "DUNS29", null, "DUNS24", 1700320L, "220", 1, "Food Production", 228 },
            { "tesla.com", "DUNS111", "DUNS111", "DUNS110", 3131213L, "1123", 3, "Legal", 229 },
            { "tesla.com", "DUNS121", "DUNS121", "DUNS120", 3131213L, "1123", 3, "Legal", 230 },
            { "tesla.com", "DUNS122", "DUNS122", null, 3131213L, "1123", 3, "Legal", 231 },
            // domain only entries : not cleaned up
            { "amazon.com", null, "DUNS17", "DUNS18", 100002421L, null, 1, "Manufacturing - Semiconductors", 232 },
            { "netappDu.com", null, "DUNS28", null, null, null, null, "X-ray Apparatus and Tubes", 233 },
            // duns only entries : not cleaned up
            { null, "DUNS43", "DUNS19", "DUNS43", 321932822L, "23019", 23, "Consumer Services", 234 },
            { null, "DUNS69", null, "DUNS69", 231131L, "1313", 2, "Non-profit", 235 },
            // added entries for orb cleanup for category
            { "paypal.com", "DUNS75", "DUNS75", null, 37875812L, "2425", 341, "Legal", 238 },
            { "netsuite.com", "DUNS890", "DUNS900", null, 32847L, "4547", 13, "Media", 236 },
            { "paypalHQ.com", "DUNS891", "DUNS891", null, 23284781L, "447", 3, "Media", 237 },
            { null, "DUNS76", "DUNS891", null, 3787581L, "2425", 341, "Legal", 239 },
            // dont cleanup to avoid missing root duns
            { "sbiDuns2.com", "DUNS14", "DUNS10", "DUNS11", 500002499L, "6500", 3, "Legal", 216 },
            { "rubrik.com", "DUNS89", "DUNS900", null, 126612L, "4547", 13, "Media", 241 },
            { "rubrik.com", "DUNS70", "DUNS75", null, 128312L, "2133", 22, "Legal", 240 },
    };

    Object[][] orbSecSrcCleanedupValues = new Object[][] { //
            // PrimaryDomain, SecondaryDomain
            // PriRootDuns != null, SecRootDuns != null, PriRootDuns == SecRootDuns
            { "netappDuns1.com", "karlDuns2.com" }, { "netappDuns1.com", "craigslist.com" },
            { "mongoDbDuns1.com", "netappDuns2.com" }, { "worldwildlife.org", "unicef.org" },
            { "socialorg.com", "goodwill.com" }
    };

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = null;
        switch (source) {
            case DOM_OWNERSHIP_TABLE:
                rowCount = 0;
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
                }
                Assert.assertEquals(rowCount, 29);
                break;
            case ORB_SEC_CLEANED:
                rowCount = 0;
                expectedData = new HashMap<>();
                for (Object[] data : orbSecSrcCleanedupValues) {
                    expectedData.put(String.valueOf(data[1]) + String.valueOf(data[0]), data);
                }
                while (records.hasNext()) {
                    GenericRecord record = records.next();
                    log.info("record : " + record);
                    String priDomain = String.valueOf(record.get(0));
                    String secDomain = String.valueOf(record.get(1));
                    Object[] expected = expectedData.get(priDomain + secDomain);
                    Assert.assertTrue(isObjEquals(secDomain, expected[0]));
                    Assert.assertTrue(isObjEquals(priDomain, expected[1]));
                    rowCount++;
                }
                Assert.assertEquals(rowCount, 5);
                break;
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> amSeedExpectedValues = new HashMap<>();
        for (Object[] data : amSeedCleanedUpValues) {
            amSeedExpectedValues.put(String.valueOf(data[0]) + String.valueOf(data[1]), data);
        }
        String[] expectedValueOrder = { "Domain", "DUNS", "GLOBAL_ULTIMATE_DUNS_NUMBER", "LE_PRIMARY_DUNS",
                "SALES_VOLUME_US_DOLLARS", "EMPLOYEES_TOTAL", "LE_NUMBER_OF_LOCATIONS", "PrimaryIndustry",
                "AlexaRank" };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : " + record);
            String domain = String.valueOf(record.get("Domain"));
            String duns = String.valueOf(record.get("DUNS"));
            Object[] expectedVal = amSeedExpectedValues.get(domain + duns);
            for (int i = 0; i < expectedValueOrder.length; i++) {
                Assert.assertTrue(isObjEquals(record.get(expectedValueOrder[i]), expectedVal[i]));
            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, 42);
    }
}
