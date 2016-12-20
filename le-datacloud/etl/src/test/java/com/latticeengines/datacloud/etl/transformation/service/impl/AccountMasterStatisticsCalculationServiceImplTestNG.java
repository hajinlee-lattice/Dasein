package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterStatisticsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public class AccountMasterStatisticsCalculationServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    @Autowired
    PipelineSource source;

    @Autowired
    AccountMaster baseSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "MatchResult";
    String targetVersion;

    @Test(groups = "functional", enabled=false)
    public void testTransformation() {
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
        return hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        ObjectMapper om = new ObjectMapper();

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add("AccountMaster");
        step1.setBaseSources(baseSources);
        step1.setTransformer("sourceDeduper");
        String deduperConfig = getDeduperConfig();
        step1.setConfiguration(deduperConfig);

        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<Integer>();
        inputSteps.add(0);
        step2.setInputSteps(inputSteps);
        step2.setTargetSource("AMStatsResult");
        step2.setTransformer("statisticsDataTransformer");

        AccountMasterStatisticsConfig confParam = getAccountMasterStatsParameters();
        String confParamStr = null;
        try {
            confParamStr = om.writeValueAsString(confParam);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step2.setConfiguration(confParamStr);

        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);

        configuration.setSteps(steps);

        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        return configuration;
    }

    private String getDeduperConfig() {
        return "{\"DedupeField\" : \"LDC_DUNS\"}";
    }

    private AccountMasterStatisticsConfig getAccountMasterStatsParameters() {
        AccountMasterStatisticsConfig param = new AccountMasterStatisticsConfig();
        Map<String, String> attributeCategoryMap = new HashMap<>();
        param.setAttributeCategoryMap(attributeCategoryMap);
        Map<String, Map<String, Long>> dimensionValuesIdMap = new HashMap<>();
        param.setDimensionValuesIdMap(dimensionValuesIdMap);
        param.setCubeColumnName("EncodedCube");

        List<String> dimensions = new ArrayList<>();
        dimensions.add("Location");
        dimensions.add("Industry");
        param.setDimensions(dimensions);
        return param;
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSource, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }
}
