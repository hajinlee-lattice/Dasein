package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.OrbCompanyRaw;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

import edu.emory.mathcs.backport.java.util.Arrays;

public class OrbFileToSourceServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Log log = LogFactory.getLog(OrbFileToSourceServiceTestNG.class);

    @Autowired
    OrbCompanyRaw source;

    @Autowired
    IngestionSource baseSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "OrbCompanyRaw";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = false)
    public void testTransformation() {
        baseSource.setIngetionName(IngestionNames.ORB_INTELLIGENCE);
        uploadBaseSourceFile(baseSource, "orb-db2-export-sample/orb_companies.csv", baseSourceVersion);
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
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            configuration.setName("OrbCompanyFileToSource");
            configuration.setVersion(targetVersion);

            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("IngestionSource");
            step1.setBaseSources(baseSources);
            step1.setTransformer("ingestedFileToSourceTransformer");
            step1.setTargetSource(targetSourceName);
            String confParamStr1 = getIngestedFileToSourceTransformerConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    private String getIngestedFileToSourceTransformerConfig() throws JsonProcessingException {
        IngestedFileToSourceTransformerConfig conf = new IngestedFileToSourceTransformerConfig();
        conf.setIngetionName(IngestionNames.ORB_INTELLIGENCE);
        conf.setFileName("orb_companies.csv");
        return om.writeValueAsString(conf);
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        String[] expectedDuns = new String[] { "8129065", "12438907", "12221764", "17145445", "11780445", "11417478",
                "13262799", "17149076", "8825824", "116109312", "117155602", "126441554", "133303900", "117155600",
                "117155604", "109785854" };
        Set<String> expectedDunsSet = new HashSet<>(Arrays.asList(expectedDuns));
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String orbNum = record.get("OrbNum").toString();
            Assert.assertTrue(expectedDunsSet.contains(orbNum));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 16);
    }

}
