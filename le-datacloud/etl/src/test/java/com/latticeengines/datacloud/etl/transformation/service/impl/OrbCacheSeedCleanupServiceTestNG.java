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
import com.latticeengines.datacloud.core.source.impl.OrbCacheSeed;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.OrbCacheSeedSecondaryDomainAccumulationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.OrbCacheSeedSecondaryDomainCleanupTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.OrbCacheSeedSecondaryDomainMarkerTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public class OrbCacheSeedCleanupServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    @Autowired
    PipelineSource source;

    @Autowired
    OrbCacheSeed baseSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "OrbCacheSeedCleaned";
    String targetVersion;

    private static final String MARKER_FIELD_NAME = "_secondaryDomainMarker_";
    private static final String DOMAIN_FIELD_NAME = "Domain";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "OrbCacheSeed_Test", "2017-01-09_19-12-43_UTC");
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
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("OrbCacheSeed");
            step1.setBaseSources(baseSources);
            step1.setTransformer("orbCacheSeedMarkerTransformer");
            step1.setTargetSource("OrbCacheSeedMarked");
            String confParamStr1 = getMarkerConfig();
            step1.setConfiguration(confParamStr1);
            // -----------
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTargetSource("OrbCacheSeedCleaned");
            step2.setTransformer("orbCacheSeedCleanedTransformer");

            String confParamStr2 = getCleanupConfig();

            step2.setConfiguration(confParamStr2);
            // -----------
            TransformationStepConfig step3 = new TransformationStepConfig();
            step3.setInputSteps(inputSteps);
            step3.setTargetSource("OrbCacheSeedSecondaryDomain");
            step3.setTransformer("orbCacheSeedSecondaryDomainTransformer");

            String confParamStr3 = getAccumulationConfig();

            step3.setConfiguration(confParamStr3);
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            steps.add(step3);
            // -----------
            configuration.setSteps(steps);

            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    private String getCleanupConfig() {
        OrbCacheSeedSecondaryDomainCleanupTransformerConfig confParam = new OrbCacheSeedSecondaryDomainCleanupTransformerConfig();
        confParam.setMarkerFieldName(MARKER_FIELD_NAME);
        String confParamStr = null;
        try {
            confParamStr = om.writeValueAsString(confParam);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return confParamStr;
    }

    private String getAccumulationConfig() {
        OrbCacheSeedSecondaryDomainAccumulationTransformerConfig confParam = new OrbCacheSeedSecondaryDomainAccumulationTransformerConfig();
        List<String> domainMappingFields = new ArrayList<>();
        domainMappingFields.add(DOMAIN_FIELD_NAME);
        domainMappingFields.add("PrimaryDomain");
        confParam.setDomainMappingFields(domainMappingFields);
        confParam.setMarkerFieldName(MARKER_FIELD_NAME);
        confParam.setSecondaryDomainFieldName(DOMAIN_FIELD_NAME);
        confParam.setRenamedSecondaryDomainFieldName("SecondaryDomain");
        String confParamStr = null;
        try {
            confParamStr = om.writeValueAsString(confParam);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return confParamStr;
    }

    private String getMarkerConfig() throws JsonProcessingException {
        OrbCacheSeedSecondaryDomainMarkerTransformerConfig conf = new OrbCacheSeedSecondaryDomainMarkerTransformerConfig();
        conf.setMarkerFieldName(MARKER_FIELD_NAME);
        List<String> fieldsToCheck = new ArrayList<>();
        fieldsToCheck.add("IsSecondaryDomain");
        fieldsToCheck.add("DomainHasEmail");
        conf.setFieldsToCheck(fieldsToCheck);
        return om.writeValueAsString(conf);
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
