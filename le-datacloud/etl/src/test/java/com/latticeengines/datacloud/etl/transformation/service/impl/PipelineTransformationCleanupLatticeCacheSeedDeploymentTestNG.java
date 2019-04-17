package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeed;
import com.latticeengines.datacloud.core.source.impl.DomainValidation;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.LatticeCacheSeed;
import com.latticeengines.datacloud.core.source.impl.OrbCacheSeed;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceFieldEnrichmentTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceFirmoGraphEnrichmentTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceSeedFileMergeTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public class PipelineTransformationCleanupLatticeCacheSeedDeploymentTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final String VERSION = "2017-01-26_19-05-51_UTC";

    private static final String LATTICE_CACHE_SEED_CLEANED = "LatticeCacheSeed";

    private static final Logger log = LoggerFactory
            .getLogger(PipelineTransformationCleanupLatticeCacheSeedDeploymentTestNG.class);

    @Autowired
    private PipelineSource source;

    @Autowired
    private DnBCacheSeed baseDnbCacheSource;

    @Autowired
    private OrbCacheSeed orbCacheSource;

    @Autowired
    private LatticeCacheSeed baseLatticeCacheSource;

    @Autowired
    private DomainValidation domainValidationSource;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    String targetSourceName = "MatchResult";
    String targetVersion;

    @Test(groups = "deployment")
    public void testTransformation() throws IOException {
        GeneralSource baseRTSCacheSeedSource = new GeneralSource();
        baseRTSCacheSeedSource.setSourceName("RTSCacheSeed");
        uploadBaseSourceFile(baseRTSCacheSeedSource, "RTSCacheSeed_Cleanup_Test", VERSION);
        uploadBaseSourceFile(baseDnbCacheSource, "DnBCacheSeed_Cleanup_Test", VERSION);
        uploadBaseSourceFile(orbCacheSource, "OrbCacheSeed_Cleanup_Test", VERSION);
        uploadBaseSourceFile(domainValidationSource, "DomainValidation", VERSION);
        String targetSourcePath = hdfsPathBuilder.podDir().append(LATTICE_CACHE_SEED_CLEANED).toString();
        HdfsUtils.rmdir(yarnConfiguration, targetSourcePath);

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
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
        return hdfsPathBuilder.constructSnapshotDir(baseLatticeCacheSource.getSourceName(), baseSourceVersion)
                .toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {

        PipelineTransformationRequest request = new PipelineTransformationRequest();

        request.setName("LatticeCacheSeedCleanupPipeline");
        request.setVersion(VERSION);
        List<TransformationStepConfig> steps = new ArrayList<>();

        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Arrays.asList("RTSCacheSeed", "OrbCacheSeed"));
        step.setTransformer("sourceSeedFileMergeTransformer");
        step.setConfiguration(getSeedFileMergeConfig());
        steps.add(step);

        int stepSource = 0;
        step = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(stepSource++);
        step.setInputSteps(inputSteps);
        step.setBaseSources(Arrays.asList("DomainValidation"));
        step.setTransformer("standardizationTransformer");
        step.setConfiguration(
                "{\"IsValidDomainField\":\"IsValidDomain\",\"ValidDomainCheckField\":\"Domain\",\"Sequence\":[\"VALID_DOMAIN\"]}");
        steps.add(step);

        step = new TransformationStepConfig();
        inputSteps = new ArrayList<>();
        inputSteps.add(stepSource++);
        step.setInputSteps(inputSteps);
        step.setTransformer("standardizationTransformer");
        step.setConfiguration(
                "{\"FilterExpression\":\"IsValidDomain == null || IsValidDomain == true\",\"FilterFields\":[\"IsValidDomain\"],\"Sequence\":[\"FILTER\"]}");
        steps.add(step);

        step = new TransformationStepConfig();
        // step.setBaseSources(Arrays.asList("LatticeCacheSeed"));
        inputSteps = new ArrayList<>();
        inputSteps.add(stepSource++);
        step.setInputSteps(inputSteps);
        step.setTransformer("bulkMatchTransformer");
        step.setConfiguration(getMatchConfig());
        steps.add(step);

        step = new TransformationStepConfig();
        inputSteps = new ArrayList<>();
        inputSteps.add(stepSource++);
        step.setInputSteps(inputSteps);
        // step.setTargetSource(LATTICE_CACHE_SEED_CLEANED);
        step.setTransformer("sourceFieldEnrichmentTransformer");
        step.setConfiguration(getFieldsEnrichmentConfig());
        steps.add(step);

        step = new TransformationStepConfig();
        inputSteps = new ArrayList<>();
        inputSteps.add(stepSource++);
        step.setInputSteps(inputSteps);
        // step.setTargetSource(LATTICE_CACHE_SEED_CLEANED);
        step.setTransformer("sourceDedupeWithDenseFieldsTransformer");
        // step.setConfiguration("{\"DedupeFields\": [\"Domain\", \"DUNS\"],
        // \"DenseFields\": [\"City\", \"State\", \"Country\"]}");
        step.setConfiguration(
                "{\"DedupeFields\": [\"Domain\", \"DUNS\"], \"DenseFields\": [\"City\", \"State\", \"Country\"], \"SortFields\":[\"__Source_Priority__\"]}");
        steps.add(step);

        step = new TransformationStepConfig();
        step.setBaseSources(Arrays.asList("DnBCacheSeed"));
        inputSteps = new ArrayList<>();
        inputSteps.add(stepSource++);
        step.setInputSteps(inputSteps);
        step.setTargetSource(LATTICE_CACHE_SEED_CLEANED);
        step.setTransformer("sourceFirmoGraphEnrichmentTransformer");
        step.setConfiguration(getFirmoGraphEnrichmentConfig());
        steps.add(step);

        request.setSteps(steps);
        PipelineTransformationConfiguration configuration = pipelineTransformationService
                .createTransformationConfiguration(request);
        String configJson = JsonUtils.serialize(configuration);
        log.info("Transformation Cleanup Json=" + configJson);
        return configuration;
    }

    private String getSeedFileMergeConfig() {
        SourceSeedFileMergeTransformerConfig config = new SourceSeedFileMergeTransformerConfig();
        config.setSourceFieldName("__Source__");
        config.setSourcePriorityFieldName("__Source_Priority__");
        config.setSourceFieldValues(Arrays.asList("RTS", "Orb"));
        config.setSourcePriorityFieldValues(Arrays.asList("0", "4")); // RTS=0,
                                                                      // HG=1,
                                                                      // BW=2,
                                                                      // Bombora=3,
                                                                      // Orb=4
        return JsonUtils.serialize(config);
    }

    private String getFirmoGraphEnrichmentConfig() {
        SourceFirmoGraphEnrichmentTransformerConfig config = new SourceFirmoGraphEnrichmentTransformerConfig();
        config.setLeftMatchField("DUNS");
        config.setRightMatchField("DUNS_NUMBER");
        config.setEnrichingFields(Arrays.asList("BUSINESS_NAME", "STREET_ADDRESS", "CITY_NAME", "STATE_PROVINCE_NAME",
                "COUNTRY_NAME", "LE_INDUSTRY", "LE_REVENUE_RANGE", "LE_EMPLOYEE_RANGE"));
        config.setEnrichedFields(Arrays.asList("Name", "Street", "City", "State", "Country", "Industry", "RevenueRange",
                "EmployeeRange"));
        config.setGroupFields(Arrays.asList("DUNS", "Domain"));

        config.setKeepInternalColumns(true);
        return JsonUtils.serialize(config);
    }

    private String getFieldsEnrichmentConfig() {
        SourceFieldEnrichmentTransformerConfig config = new SourceFieldEnrichmentTransformerConfig();
        config.setFromFields(Arrays.asList("__Matched_DUNS__", "__Matched_City__"));
        config.setToFields(Arrays.asList("DUNS", "City"));
        return JsonUtils.serialize(config);
    }

    private String getMatchConfig() {
        MatchTransformerConfig config = new MatchTransformerConfig();
        MatchInput matchInput = new MatchInput();
        matchInput.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        matchInput.setTenant(new Tenant("MatchTransformer"));
        matchInput.setPredefinedSelection(Predefined.ID);
        matchInput.setKeyMap(getKeyMap());
        matchInput.setDecisionGraph("DragonClaw");
        matchInput.setExcludePublicDomain(false);
        matchInput.setPublicDomainAsNormalDomain(true);
        matchInput.setDataCloudVersion(getDataCloudVersion());
        matchInput.setSkipKeyResolution(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(false);
        matchInput.setLogDnBBulkResult(false);
        matchInput.setMatchDebugEnabled(true);

        config.setMatchInput(matchInput);
        return JsonUtils.serialize(config);
    }

    private String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion(null).getVersion();
    }

    private Map<MatchKey, List<String>> getKeyMap() {
        Map<MatchKey, List<String>> keyMap = new TreeMap<>();
        keyMap.put(MatchKey.Name, Arrays.asList("Name"));
        keyMap.put(MatchKey.Country, Arrays.asList("Country"));
        keyMap.put(MatchKey.State, Arrays.asList("State"));
        keyMap.put(MatchKey.City, Arrays.asList("City"));
        // keyMap.put(MatchKey.Zipcode, Arrays.asList("Zipcode"));
        // keyMap.put(MatchKey.PhoneNumber, Arrays.asList("PhoneNumber"));
        return keyMap;
    }

    @Override
    protected String getPathForResult() {
        targetSourceName = LATTICE_CACHE_SEED_CLEANED;
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = String.valueOf(record.get("ID"));
            recordMap.put(id, record);
            rowNum++;
        }
        log.info("Total result records " + rowNum);
        Assert.assertEquals(rowNum, 3);
        GenericRecord record = recordMap.get("3");
        Assert.assertEquals(record.get("City").toString(), "New JACKSON");
        Assert.assertEquals(record.get("State").toString(), "New CA10");

        Assert.assertTrue(record != null);
        record = recordMap.get("6");
        Assert.assertEquals(record.get("City").toString(), "New MOUNTAIN VIEW");
        Assert.assertEquals(record.get("State").toString(), "New CA0");

        Assert.assertTrue(record != null);
        record = recordMap.get("7");
        Assert.assertEquals(record.get("City").toString(), "Menlo Park");
        Assert.assertEquals(record.get("State").toString(), "California");

    }
}
