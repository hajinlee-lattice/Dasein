package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TestIterativeStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.IterativeStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class IterativeStepTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MiniAMDomainDunsTestNG.class);
    GeneralSource source = new GeneralSource("DummyDataSet");
    GeneralSource baseSource = new GeneralSource("dataSet2");

    String targetSourceName = source.getSourceName();
    ObjectMapper om = new ObjectMapper();
    
    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        prepareDataSet1();
        prepareDataSet2();
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
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("DummyDataSet");
            configuration.setVersion(targetVersion);

            // Initialize Dummy Data Sets
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourcesStep1 = new ArrayList<String>();
            baseSourcesStep1.add(source.getSourceName());
            baseSourcesStep1.add(baseSource.getSourceName());
            step1.setBaseSources(baseSourcesStep1);
            step1.setTransformer("DummyIterativeTransformer");
            step1.setStepType(TransformationStepConfig.ITERATIVE);
            String confParamStr1 = getDummyDataConfig();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(targetSourceName);

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

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record : " + record);
            rowCount++;
        }
        log.info("rowCount : " + rowCount);
        Assert.assertTrue((rowCount % 2 == 0));
    }

    private void prepareDataSet1() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        Object[][] data = new Object[][] {
                { "amazon.com" }, { "kaggle.com" }
        };
        uploadBaseSourceData(source.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareDataSet2() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        Object[][] data = new Object[][] {
                { "google.com" }, { "netapp.com" }
        };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    private String getDummyDataConfig() throws JsonProcessingException {
        TestIterativeStepConfig conf = new TestIterativeStepConfig();
        IterativeStepConfig.ConvergeOnCount iterateStrategy = new IterativeStepConfig.ConvergeOnCount();
        iterateStrategy.setIteratingSource(targetSourceName);
        iterateStrategy.setCountDiff(0);
        conf.setIterateStrategy(iterateStrategy);
        return om.writeValueAsString(conf);
    }

}
