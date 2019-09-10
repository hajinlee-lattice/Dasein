package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.MapAttributeFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.MapAttributeTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MapAttributeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccountMasterRebuildRefreshTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AccountMasterRebuildRefreshTestNG.class);

    private static final String VERSION1 = "2017-06-01_00-00-00_UTC";
    private static final String VERSION2 = "2017-07-01_00-00-00_UTC";

    @Autowired
    private AccountMaster source;
    private GeneralSource ams = new GeneralSource("AccountMasterSeed");
    private GeneralSource alexa = new GeneralSource("AlexaMostRecent");
    private GeneralSource bombora30 = new GeneralSource("Bombora30DayAgg");
    private GeneralSource bomboraSurge = new GeneralSource("BomboraSurgePivoted");
    private GeneralSource bomboraSurgeRLS = new GeneralSource("BomboraSurgePivotedRLS");
    private GeneralSource bwPivoted = new GeneralSource("BuiltWithPivoted");
    private GeneralSource bwTech = new GeneralSource("BuiltWithTechIndicators");
    private GeneralSource feature = new GeneralSource("FeaturePivoted");
    private GeneralSource hgPivoted = new GeneralSource("HGDataPivoted");
    private GeneralSource hgTech = new GeneralSource("HGDataTechIndicators");
    private GeneralSource hpa = new GeneralSource("HPANewPivoted");
    private GeneralSource orb = new GeneralSource("OrbIntelligenceMostRecent");
    private GeneralSource semrush = new GeneralSource("SemrushMostRecent");
    private GeneralSource dnb = new GeneralSource("DnBCacheSeed");

    private Map<String[], Map<String, String[]>> keyMap;    // ams keys -> (source -> keys)
    private Map<String[], Map<String, String[]>> keyMapRefresh;    // am keys -> (source -> keys) 

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

    @Test(groups = "functional")
    public void testTransformation() {
        prepareBaseSources();
        prepareKeyMap();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(source, VERSION1);
        confirmIntermediateSource(source, VERSION2);
        confirmSchema();
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("AMRebuildRefresh");
        configuration.setVersion(targetVersion);

        // -----------------
        TransformationStepConfig step1 = new TransformationStepConfig();
        step1.setTargetSource(source.getSourceName());
        step1.setTargetVersion(VERSION1);
        String[] baseSourceArr = { ams.getSourceName(), dnb.getSourceName(), alexa.getSourceName(),
                bombora30.getSourceName(), bomboraSurge.getSourceName(), bwPivoted.getSourceName(),
                bwTech.getSourceName(), feature.getSourceName(), hgPivoted.getSourceName(), hgTech.getSourceName(),
                hpa.getSourceName(), orb.getSourceName(), semrush.getSourceName() };
        String[] baseVersions = { VERSION1, VERSION1, VERSION1, VERSION1, VERSION1, VERSION1, VERSION1, VERSION1,
                VERSION1, VERSION1, VERSION1, VERSION1, VERSION1 };
        step1.setBaseVersions(Arrays.asList(baseVersions));
        List<String> baseSources = new ArrayList<>(Arrays.asList(baseSourceArr));
        step1.setBaseSources(baseSources);
        step1.setTransformer(MapAttributeFlow.MAP_TRANSFORMER);
        String confParamStr1 = getMapperConfig(baseSources, true);
        step1.setConfiguration(confParamStr1);

        // -----------------
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setTargetSource(source.getSourceName());
        step2.setTargetVersion(VERSION2);
        String[] baseSourceArr2 = { source.getSourceName(), bomboraSurgeRLS.getSourceName() };
        String[] baseVersions2 = { VERSION1, VERSION2 };
        step2.setBaseVersions(Arrays.asList(baseVersions2));
        List<String> baseSources2 = new ArrayList<>(Arrays.asList(baseSourceArr2));
        step2.setBaseSources(baseSources2);
        step2.setTransformer(MapAttributeFlow.MAP_TRANSFORMER);
        String confParamStr2 = getMapperConfig(baseSources2, false);
        step2.setConfiguration(confParamStr2);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getMapperConfig(List<String> templates, boolean isRebuild) {
        MapAttributeConfig config = new MapAttributeConfig();
        config.setSource(source.getSourceName());
        config.setTemplates(templates);
        config.setSeed(templates.get(0));
        config.setSeedId("LatticeID");
        if (isRebuild) {
            config.setStage(MapAttributeFlow.MAP_STAGE);
            config.setJoinConfigs(createJoinConfigs(keyMap));
        } else {
            config.setStage(MapAttributeFlow.REFRESH_STAGE);
            config.setJoinConfigs(createJoinConfigs(keyMapRefresh));
            DataCloudVersion currentVersion = dataCloudVersionService.currentApprovedVersion();
            String nextVersion = dataCloudVersionService.nextMinorVersion(currentVersion.getVersion());
            config.setDataCloudVersion(nextVersion);
        }

        return JsonUtils.serialize(config);
    }

    private List<MapAttributeConfig.JoinConfig> createJoinConfigs(Map<String[], Map<String, String[]>> keyMap) {
        List<MapAttributeConfig.JoinConfig> joinConfigs = new ArrayList<MapAttributeConfig.JoinConfig>();
        for (Map.Entry<String[], Map<String, String[]>> keyEnt : keyMap.entrySet()) {
            MapAttributeConfig.JoinConfig joinConfig = new MapAttributeConfig.JoinConfig();
            joinConfig.setKeys(Arrays.asList(keyEnt.getKey()));
            List<MapAttributeConfig.JoinTarget> joinTargets = new ArrayList<MapAttributeConfig.JoinTarget>();
            for (Map.Entry<String, String[]> ent : keyEnt.getValue().entrySet()) {
                MapAttributeConfig.JoinTarget joinTarget = new MapAttributeConfig.JoinTarget();
                joinTarget.setSource(ent.getKey());
                joinTarget.setKeys(Arrays.asList(ent.getValue()));
                joinTargets.add(joinTarget);
            }
            joinConfig.setTargets(joinTargets);
            joinConfigs.add(joinConfig);
        }
        return joinConfigs;
    }

    private void prepareKeyMap() {
        keyMap = new HashMap<>();
        Map<String, String[]> domKeyMap = new HashMap<>();
        Map<String, String[]> dunsKeyMap = new HashMap<>();
        domKeyMap.put(alexa.getSourceName(), new String[] { "URL" });
        domKeyMap.put(bombora30.getSourceName(), new String[] { "Domain" });
        domKeyMap.put(bomboraSurge.getSourceName(), new String[] { "Domain" });
        domKeyMap.put(bwPivoted.getSourceName(), new String[] { "Domain" });
        domKeyMap.put(bwTech.getSourceName(), new String[] { "Domain" });
        domKeyMap.put(feature.getSourceName(), new String[] { "URL" });
        domKeyMap.put(hgPivoted.getSourceName(), new String[] { "Domain" });
        domKeyMap.put(hgTech.getSourceName(), new String[] { "Domain" });
        domKeyMap.put(hpa.getSourceName(), new String[] { "URL" });
        domKeyMap.put(orb.getSourceName(), new String[] { "Domain" });
        domKeyMap.put(semrush.getSourceName(), new String[] { "Domain" });
        dunsKeyMap.put(dnb.getSourceName(), new String[] { "DUNS_NUMBER" });
        keyMap.put(new String[] { "LDC_Domain" }, domKeyMap);
        keyMap.put(new String[] { "LDC_DUNS" }, dunsKeyMap);

        keyMapRefresh = new HashMap<>();
        Map<String, String[]> domKeyMapRefresh = new HashMap<>();
        domKeyMapRefresh.put(bomboraSurgeRLS.getSourceName(), new String[] { "Domain" });
        keyMapRefresh.put(new String[] { "LDC_Domain" }, domKeyMapRefresh);
    }

    private void prepareBaseSources() {
        prepareAMSeed();
        prepareAlexaMostRecent();
        prepareBombora30();
        prepareBomboraSurge();
        prepareBWPivoted();
        prepareBWTech();
        prepareFeature();
        prepareHGPivoted();
        prepareHGTech();
        prepareHPA();
        prepareOrb();
        prepareSemrush();
        prepareDnB();
    }

    private void prepareAMSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeID", Long.class));
        columns.add(Pair.of("LDC_Domain", String.class));
        columns.add(Pair.of("LDC_DUNS", String.class));
        columns.add(Pair.of("LDC_Name", String.class));
        Object[][] data = new Object[][] { //
                { 1L, "dom1.com", "DUNS1", "Name1" }, //
                { 2L, "dom2.com", "DUNS2", "Name2" }, //
                { 3L, "dom1.com", "DUNS2", "Name2" }, //
                { 4L, null, "DUNS3", "Name3" }, //
                { 5L, "dom3.com", null, "Name3" }, //
        };
        uploadBaseSourceData(ams.getSourceName(), VERSION1, columns, data);
    }

    private void prepareAlexaMostRecent() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("URL", String.class));
        columns.add(Pair.of("Rank", Integer.class));
        Object[][] data = new Object[][] { //
                { "dom1.com", 10000 }, //
                { "dom3.com", 1000000 }, //
        };
        uploadBaseSourceData(alexa.getSourceName(), VERSION1, columns, data);
    }

    private void prepareBombora30() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("ML_30Day_Healthcare_Total", Integer.class));
        Object[][] data = new Object[][] { //
                { "dom2.com", 50241 }, //
        };
        uploadBaseSourceData(bombora30.getSourceName(), VERSION1, columns, data);
    }

    private void prepareBomboraSurge() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("BmbrSurge_Intent", String.class));
        Object[][] data = new Object[][] { //
                { "dom3.com", "ABC" }, //
        };
        Object[][] dataRefreshed = new Object[][] { //
                { "dom1.com", "ABC" }, //
                { "dom2.com", "DEF" }, //
                { "dom3.com", "GHI" }, //
        };
        uploadBaseSourceData(bomboraSurge.getSourceName(), VERSION1, columns, data);
        uploadBaseSourceData(bomboraSurgeRLS.getSourceName(), VERSION2, columns, dataRefreshed);
    }

    private void prepareBWPivoted() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("BusinessTechnologiesAds", Integer.class));
        Object[][] data = new Object[][] { //
                { "dom1.com", 50 }, //
        };
        uploadBaseSourceData(bwPivoted.getSourceName(), VERSION1, columns, data);
    }

    private void prepareBWTech() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("TechIndicators", String.class));
        Object[][] data = new Object[][] { //
                { "dom1.com", "12345" }, //
                { "dom2.com", "67890" }, //
        };
        uploadBaseSourceData(bwTech.getSourceName(), VERSION1, columns, data);
    }

    private void prepareFeature() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("URL", String.class));
        columns.add(Pair.of("Term_Add_Item", Integer.class));
        Object[][] data = new Object[][] { //
                { "dom1.com", 123 }, //
                { "dom2.com", 456 }, //
                { "dom3.com", 789 }, //
        };
        uploadBaseSourceData(feature.getSourceName(), VERSION1, columns, data);
    }

    private void prepareHGPivoted() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("CloudTechnologies_SRM", Integer.class));
        Object[][] data = new Object[][] { //
                { "dom1.com", 321 }, //
                { "dom3.com", 987 }, //
        };
        uploadBaseSourceData(hgPivoted.getSourceName(), VERSION1, columns, data);
    }

    private void prepareHGTech() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("SegmentTechIndicators", String.class));
        Object[][] data = new Object[][] { //
                { "dom2.com", "QWER" }, //
                { "dom3.com", "ASDF" }, //
        };
        uploadBaseSourceData(hgTech.getSourceName(), VERSION1, columns, data);
    }

    private void prepareHPA() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("URL", String.class));
        columns.add(Pair.of("Email_Suffix", String.class));
        Object[][] data = new Object[][] { //
                { "dom2.com", "google.com" }, //
        };
        uploadBaseSourceData(hpa.getSourceName(), VERSION1, columns, data);
    }

    private void prepareOrb() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("Total_Amount_Raised", Long.class));
        Object[][] data = new Object[][] { //
                { "dom3.com", 100000000L }, //
        };
        uploadBaseSourceData(orb.getSourceName(), VERSION1, columns, data);
    }

    private void prepareSemrush() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("Rank", Integer.class));
        Object[][] data = new Object[][] { //
                { "dom1.com", 112233 }, //
        };
        uploadBaseSourceData(semrush.getSourceName(), VERSION1, columns, data);
    }

    private void prepareDnB() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("DUNS_NUMBER", String.class));
        columns.add(Pair.of("SALES_VOLUME_LOCAL_CURRENCY", Long.class));
        Object[][] data = new Object[][] { //
                { "DUNS1", 111222333L }, //
                { "DUNS2", 444555666L }, //
                { "DUNS3", 777888999L }, //
        };
        uploadBaseSourceData(dnb.getSourceName(), VERSION1, columns, data);
    }

    private Object[][] amRebuildData = { //
            { 1L, "dom1.com", "DUNS1", "Name1", 10000, null, null, 50, "12345", 123, 321, null, null, null, 112233,
                    111222333L }, //
            { 2L, "dom2.com", "DUNS2", "Name2", null, 50241, null, null, "67890", 456, null, "QWER",
                    "google.com", null,
                    null, 444555666L }, //
            { 3L, "dom1.com", "DUNS2", "Name2", 10000, null, null, 50, "12345", 123, 321, null, null, null, 112233,
                    444555666L }, //
            { 4L, null, "DUNS3", "Name3", null, null, null, null, null, null, null, null, null, null, null,
                    777888999L }, //
            { 5L, "dom3.com", null, "Name3", 1000000, null, "ABC", null, null, 789, 987, "ASDF",
                    null, 100000000L, null, null }, //
    };

    private Object[][] amRefreshData = { //
            { 1L, "dom1.com", "DUNS1", "Name1", 10000, null, "ABC", 50, "12345", 123, 321, null, null, null, 112233,
                    111222333L }, //
            { 2L, "dom2.com", "DUNS2", "Name2", null, 50241, "DEF", null, "67890", 456, null, "QWER", "google.com",
                    null, null, 444555666L }, //
            { 3L, "dom1.com", "DUNS2", "Name2", 10000, null, "ABC", 50, "12345", 123, 321, null, null, null, 112233,
                    444555666L }, //
            { 4L, null, "DUNS3", "Name3", null, null, null, null, null, null, null, null, null, null, null,
                    777888999L }, //
            { 5L, "dom3.com", null, "Name3", 1000000, null, "GHI", null, null, 789, 987, "ASDF",
                    null, 100000000L, null, null }, //
    };

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info(String.format("Start to verify source %s @%s", source, version));
        Map<Long, Object[]> expectedData = new HashMap<>();
        switch (version) {
        case VERSION1:
            for (Object[] data : amRebuildData) {
                expectedData.put((Long) data[0], data);
            }
            break;
        case VERSION2:
            for (Object[] data : amRefreshData) {
                expectedData.put((Long) data[0], data);
            }
            break;
        default:
            throw new RuntimeException(String.format("Unexpected version for AccountMaster: %s", version));
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Long id = (Long) record.get("LatticeID");
            Object[] expected = expectedData.get(id);
            Assert.assertTrue(isObjEquals(record.get("LDC_Domain"), expected[1]));
            Assert.assertTrue(isObjEquals(record.get("LDC_DUNS"), expected[2]));
            Assert.assertTrue(isObjEquals(record.get("LDC_Name"), expected[3]));
            Assert.assertTrue(isObjEquals(record.get("AlexaRank"), expected[4]));
            Assert.assertTrue(isObjEquals(record.get("Bmbr30_Healthcare_Total"), expected[5]));
            Assert.assertTrue(isObjEquals(record.get("BmbrSurge_Intent"), expected[6]));
            Assert.assertTrue(isObjEquals(record.get("BusinessTechnologiesAds"), expected[7]));
            Assert.assertTrue(isObjEquals(record.get("BuiltWith_TechIndicators"), expected[8]));
            Assert.assertTrue(isObjEquals(record.get("FeatureTermAddItem"), expected[9]));
            Assert.assertTrue(isObjEquals(record.get("CloudTechnologies_SRM"), expected[10]));
            Assert.assertTrue(isObjEquals(record.get("HGData_SegmentTechIndicators"), expected[11]));
            Assert.assertTrue(isObjEquals(record.get("HPAEmailSuffix"), expected[12]));
            Assert.assertTrue(isObjEquals(record.get("Total_Amount_Raised"), expected[13]));
            Assert.assertTrue(isObjEquals(record.get("SemrushRank"), expected[14]));
            Assert.assertTrue(isObjEquals(record.get("SALES_VOLUME_LOCAL_CURRENCY"), expected[15]));
        }
    }

    private void confirmSchema() {
        Schema rebuildSchema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(source, VERSION1);
        Schema refreshSchema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(source, VERSION2);
        String currentVersion = dataCloudVersionService.currentApprovedVersion().getVersion();
        String nextVersion = dataCloudVersionService.nextMinorVersion(currentVersion);
        Assert.assertEquals(rebuildSchema.getProp(MapAttributeTransformer.DATA_CLOUD_VERSION), nextVersion);
        Assert.assertEquals(refreshSchema.getProp(MapAttributeTransformer.DATA_CLOUD_VERSION), nextVersion);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
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
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }
}
