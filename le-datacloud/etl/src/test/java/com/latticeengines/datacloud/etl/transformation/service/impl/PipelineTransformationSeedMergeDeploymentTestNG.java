package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.LatticeCacheSeed;
import com.latticeengines.datacloud.core.source.impl.OrbCacheSeed;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceFieldSelectionTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceSeedFileMergeTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class PipelineTransformationSeedMergeDeploymentTestNG extends
        TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final String VERSION = "2017-01-26_19-05-51_UTC";

    private static final String LATTICE_CACHE_SEED_MERGED = "LatticeCacheSeedMerged";

    private static final Logger log = LoggerFactory.getLogger(PipelineTransformationSeedMergeDeploymentTestNG.class);

    @Autowired
    private PipelineSource source;

    @Autowired
    private OrbCacheSeed orbCacheSource;

    @Autowired
    private LatticeCacheSeed baseLatticeCacheSource;

    String targetSourceName = "MatchResult";
    String targetVersion;

    @Test(groups = "deployment")
    public void testTransformation() throws IOException {
        GeneralSource baseRTSCacheSeedSource = new GeneralSource();
        baseRTSCacheSeedSource.setSourceName("MatchHistory");
        uploadBaseSourceFile(baseRTSCacheSeedSource, "MatchHistory_Test", VERSION);
        uploadBaseSourceFile(orbCacheSource, "OrbCacheSeed_Cleanup_Test", VERSION);
        String targetSourcePath = hdfsPathBuilder.podDir().append(LATTICE_CACHE_SEED_MERGED).toString();
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

        request.setName("LatticeCacheSeedMergePipeline");
        request.setVersion(VERSION);
        List<TransformationStepConfig> steps = new ArrayList<>();

        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Arrays.asList("MatchHistory"));
        step.setTransformer("sourceFieldSelectionTransformer");

        step.setConfiguration(getFieldSelectionConfig());
        steps.add(step);

        List<Integer> inputSteps = new ArrayList<>();
        step = new TransformationStepConfig();
        step.setBaseSources(Arrays.asList("OrbCacheSeed"));
        inputSteps = new ArrayList<>();
        inputSteps.add(0);
        step.setInputSteps(inputSteps);
        step.setTransformer("sourceSeedFileMergeTransformer");
        step.setTargetSource(LATTICE_CACHE_SEED_MERGED);
        step.setConfiguration(getSeedFileMergeConfig());
        steps.add(step);

        request.setSteps(steps);
        PipelineTransformationConfiguration configuration = pipelineTransformationService
                .createTransformationConfiguration(request);
        String configJson = JsonUtils.serialize(configuration);
        log.info("Transformation Seed Merge Json=" + configJson);
        return configuration;
    }

    private String getFieldSelectionConfig() {
        SourceFieldSelectionTransformerConfig config = new SourceFieldSelectionTransformerConfig();
        config.setNewFields(Arrays.asList("RevenueRange", "EmployeeRange", "Industry"));
        Map<String, String> renameFieldMap = new HashMap<>();
        renameFieldMap.put("Zipcode", "ZipCode");
        config.setRenameFieldMap(renameFieldMap);
        config.setRetainFields(Arrays.asList("ID", "Domain", "Name", "Country", "State", "City", "Street", "ZipCode",
                "PhoneNumber", "RevenueRange", "EmployeeRange", "Industry", "DUNS"));
        return JsonUtils.serialize(config);
    }

    private String getSeedFileMergeConfig() {
        SourceSeedFileMergeTransformerConfig config = new SourceSeedFileMergeTransformerConfig();
        config.setSourceFieldName("__Source__");
        config.setSourcePriorityFieldName("__Source_Priority__");
        config.setSourceFieldValues(Arrays.asList("Unmatched", "Orb"));
        config.setSourcePriorityFieldValues(Arrays.asList("5", "4")); // RTS=0,
                                                                      // HG=1,
                                                                      // BW=2,
                                                                      // Bombora=3,
                                                                      // Orb=4
                                                                      // Unmatched=5
        return JsonUtils.serialize(config);
    }

    @Override
    protected String getPathForResult() {
        targetSourceName = LATTICE_CACHE_SEED_MERGED;
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
        Assert.assertEquals(rowNum, 5);
        GenericRecord record = recordMap.get("7");
        Assert.assertEquals(record.get("Domain").toString(), "Facebook.com");
        Assert.assertEquals(record.get("City").toString(), "Menlo Park");
        Assert.assertEquals(record.get("State").toString(), "California");

        record = recordMap.get("111111");
        Assert.assertEquals(record.get("Domain").toString(), "shell.com");
        Assert.assertEquals(record.get("DUNS").toString(), "407888804");

        record = recordMap.get("222222");
        Assert.assertEquals(record.get("Domain").toString(), "salesforce.com");
        Assert.assertEquals(record.get("DUNS").toString(), "222222");

    }
}
