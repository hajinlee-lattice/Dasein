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

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.MockDataFlowTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class MockWorkFlowTest
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MockWorkFlowTest.class);
    private GeneralSource baseSource = new GeneralSource("baseSource");
    private GeneralSource source = new GeneralSource("TargetSource");

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
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion)
                .toString();
    }

    @Test(groups = "functional")
    public void testTransformation() throws Exception {
        for (int i = 0; i < 2; i++) {
            mockData();
            TransformationProgress progress = createNewProgress();
            progress = transformData(progress);
            finish(progress);
            confirmResultFile(progress);
            cleanupProgressTables();
        }
    }

    private Object[][] mockData = new Object[][] {
            // Col1, Col2
            { "1", "2" }, { "3", "4" }, { "5", "6" }, { "7", "8" }, };

    private void mockData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("Col1", String.class));
        schema.add(Pair.of("Col2", String.class));
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, schema, mockData);
        try {
            extractSchema(baseSource, baseSourceVersion,
                    hdfsPathBuilder
                            .constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion)
                            .toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s",
                    baseSource.getSourceName(), baseSourceVersion));
        }
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("MockTest");
            configuration.setVersion("2019-04-11_20-47-00_UTC");

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourcesStep1 = new ArrayList<String>();
            baseSourcesStep1.add(baseSource.getSourceName());
            step1.setBaseSources(baseSourcesStep1);
            step1.setTransformer(MockDataFlowTransformer.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion)
                .toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        boolean expectedValue = false;
        Assert.assertTrue(pipelineTransformationService.hdfsSourceEntityMgr.checkSourceExist(source,
                "2019-04-11_20-47-00_UTC") == expectedValue);
    }
}
