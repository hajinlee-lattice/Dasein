package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeedMerged;
import com.latticeengines.datacloud.core.source.impl.AlexaMostRecent;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public class AccountMasterSeedRebuildServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    @Autowired
    PipelineSource source;

    @Autowired
    AccountMasterSeedMerged baseSource;

    @Autowired
    AlexaMostRecent baseSource2;

    @Autowired
    AccountMasterSeed accountMasterSeedSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterSeed";
    String targetVersion;

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "AccountMasterIntermediateSeed_TestAccountMasterSeed",
                "2017-01-09_19-12-43_UTC");
        uploadBaseSourceFile(baseSource2,
                baseSource2.getSourceName() + "_Test" + accountMasterSeedSource.getSourceName(),
                "2017-01-09_19-12-43_UTC");
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
        return hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource.getSourceName());
            baseSources.add(baseSource2.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer("accountMasterSeedMarkerTransformer");
            step1.setTargetSource("AccountMasterSeedMarked");
            String confParamStr1 = getMarkerConfig();
            step1.setConfiguration(confParamStr1);
            // -----------
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTargetSource(targetSourceName);
            step2.setTransformer("accountMasterSeedCleanupTransformer");

            String confParamStr2 = getCleanupConfig();

            step2.setConfiguration(confParamStr2);
            // -----------
            TransformationStepConfig step3 = new TransformationStepConfig();
            step3.setInputSteps(inputSteps);
            step3.setTargetSource("AccountMasterSeedReport");
            step3.setTransformer("accountMasterSeedReportTransformer");

            String confParamStr3 = getReportConfig();

            step3.setConfiguration(confParamStr3);
            // // -----------
            // TransformationStepConfig step4 = new TransformationStepConfig();
            // step4.setInputSteps(inputSteps);
            // step4.setTargetSource("AccountMasterSeedSecondaryDomain");
            // step4.setTransformer("accountMasterSeedSecondaryDomainTransformer");
            //
            // String confParamStr4 = getReportConfig();
            //
            // step4.setConfiguration(confParamStr4);
            // -----------
            TransformationStepConfig step5 = new TransformationStepConfig();
            step5.setInputSteps(inputSteps);
            step5.setTargetSource("AccountMasterSeedJunkyard");
            step5.setTransformer("accountMasterSeedJunkyardTransformer");

            String confParamStr5 = getJunkyardConfig();

            step5.setConfiguration(confParamStr5);
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            steps.add(step3);
            // steps.add(step4);
            steps.add(step5);
            // -----------
            configuration.setSteps(steps);

            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    private String getCleanupConfig() throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getReportConfig() throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getJunkyardConfig() throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    private String getMarkerConfig() throws JsonProcessingException {
        AccountMasterSeedMarkerConfig conf = new AccountMasterSeedMarkerConfig();
        return om.writeValueAsString(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }
}
