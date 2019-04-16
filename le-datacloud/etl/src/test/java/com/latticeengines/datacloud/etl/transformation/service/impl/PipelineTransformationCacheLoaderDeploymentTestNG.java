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
import com.latticeengines.datacloud.core.source.impl.LatticeCacheSeed;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MatchTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SampleTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public class PipelineTransformationCacheLoaderDeploymentTestNG extends
        TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final String LATTICE_CACHE_SEED_LOADER = "LatticeCacheSeedCacheLoader";

    private static final Logger log = LoggerFactory.getLogger(PipelineTransformationCacheLoaderDeploymentTestNG.class);

    @Autowired
    private PipelineSource source;

    @Autowired
    private LatticeCacheSeed baseLatticeCacheSource;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    String targetSourceName = "MatchResult";
    String targetVersion;

    @Test(groups = "deployment")
    public void testTransformation() throws IOException {

        uploadBaseSourceFile(baseLatticeCacheSource, "am_cache", "2017-01-09_19-12-43_UTC");
        String targetSourcePath = hdfsPathBuilder.podDir().append(LATTICE_CACHE_SEED_LOADER).toString();
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

        request.setName("LatticeCacheSeedCacheLoaderPipeline");
        request.setVersion("2017-01-09_19-12-43_UTC");
        List<TransformationStepConfig> steps = new ArrayList<>();

        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Arrays.asList("LatticeCacheSeed"));
        step.setTransformer("sampler");
        step.setConfiguration(getSourceFileFilterConfig());
//        step.setTargetSource(LATTICE_CACHE_SEED_LOADER);
        steps.add(step);

        step = new TransformationStepConfig();
        // step.setBaseSources(Arrays.asList("LatticeCacheSeed"));
        List<Integer> inputSteps = new ArrayList<Integer>();
        inputSteps.add(0);
        step.setInputSteps(inputSteps);
        step.setTransformer("bulkMatchTransformer");
        step.setConfiguration(getMatchConfig());
        step.setTargetSource(LATTICE_CACHE_SEED_LOADER);
        steps.add(step);

        request.setSteps(steps);
        PipelineTransformationConfiguration configuration = pipelineTransformationService
                .createTransformationConfiguration(request);

        return configuration;
    }
    
    private String getSourceFileFilterConfig() {
        SampleTransformerConfig config = new SampleTransformerConfig();
        config.setFilter("DUNS == null || DUNS == \"\"");
        config.setFilterFields(Arrays.asList("DUNS"));
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
        matchInput.setMatchDebugEnabled(false);

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
        targetSourceName = LATTICE_CACHE_SEED_LOADER;
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSource.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        @SuppressWarnings("unused")
        Map<Long, GenericRecord> recordMap =new HashMap<>();
        while (records.hasNext()) {
            records.next();
            rowNum++;
        }
        log.info("Total result records " + rowNum);
        Assert.assertEquals(rowNum, 17);
        
    }
}
