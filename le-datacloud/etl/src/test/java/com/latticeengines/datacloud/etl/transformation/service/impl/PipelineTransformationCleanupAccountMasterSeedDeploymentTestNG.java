package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SourceDomainCleanupByDuTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class PipelineTransformationCleanupAccountMasterSeedDeploymentTestNG extends
        TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final String ACCOUNT_MASTER_SEED_CLEANED = "AccountMasterSeedCleaned";

    private static final Log log = LogFactory
            .getLog(PipelineTransformationCleanupAccountMasterSeedDeploymentTestNG.class);

    @Autowired
    PipelineSource source;

    @Autowired
    AccountMasterSeed baseAccountMasterSeedSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "MatchResult";
    String targetVersion;

    @Test(groups = "deployment")
    public void testTransformation() throws IOException {
        uploadBaseSourceFile(baseAccountMasterSeedSource, "AccountMasterSeed_Cleanup_Test", "2017-01-09_19-12-43_UTC");
        String targetSourcePath = hdfsPathBuilder.podDir().append(ACCOUNT_MASTER_SEED_CLEANED).toString();
        HdfsUtils.rmdir(yarnConfiguration, targetSourcePath);

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(baseAccountMasterSeedSource.getSourceName(), baseSourceVersion)
                .toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {

        PipelineTransformationRequest request = new PipelineTransformationRequest();

        request.setName("AccountMasterSeedCleanupPipeline");
        request.setVersion("2017-01-09_19-12-43_UTC");
        List<TransformationStepConfig> steps = new ArrayList<>();

        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Collections.singletonList("AccountMasterSeed"));
        step.setTransformer("sourceDomainCleanupByDuTransformer");
        step.setConfiguration(getCleanupByDuConfig());
        step.setTargetSource(ACCOUNT_MASTER_SEED_CLEANED);
        steps.add(step);

        request.setSteps(steps);
        PipelineTransformationConfiguration configuration = pipelineTransformationService
                .createTransformationConfiguration(request);
        String configJson =  JsonUtils.serialize(configuration);
        log.info("Transformation Cleanup Json=" + configJson);
        return configuration;
    }

    private String getCleanupByDuConfig() {
        SourceDomainCleanupByDuTransformerConfig config = new SourceDomainCleanupByDuTransformerConfig();
        config.setDuField("LE_PRIMARY_DUNS");
        config.setDunsField("DUNS");
        config.setDomainField("Domain");
        config.setAlexaRankField("AlexaRank");;
        return JsonUtils.serialize(config);
    }

    @Override
    protected String getPathForResult() {
        targetSourceName = ACCOUNT_MASTER_SEED_CLEANED;
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        Map<String, GenericRecord> recordMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = String.valueOf(record.get("LatticeID"));
            recordMap.put(id, record);
            rowNum++;
        }
        
        log.info("Total result records " + rowNum);
        Assert.assertEquals(rowNum, 14);
        
        GenericRecord record = recordMap.get("3");
        Assert.assertEquals(record.get("Domain").toString(), "google.com");
        record = recordMap.get("5");
        Assert.assertEquals(record.get("Domain").toString(), "lattice.com");
        record = recordMap.get("14");
        Assert.assertEquals(record.get("Domain").toString(), "oracle1.com");
    }
}
