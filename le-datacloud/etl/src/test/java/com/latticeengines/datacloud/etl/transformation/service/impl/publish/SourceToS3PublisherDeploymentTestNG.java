package com.latticeengines.datacloud.etl.transformation.service.impl.publish;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.TransformationServiceImplTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.publish.SourceToS3Publisher;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceToS3PublisherDeploymentTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    // private static final Logger log =
    // LoggerFactory.getLogger(SourceToS3PublisherDeploymentTestNG.class);


    private GeneralSource baseSourceAccMaster = new GeneralSource("AccountMasterVerify");

    GeneralSource source = new GeneralSource("LDCDEV_SourceToS3Publisher");

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        // verify result()
        cleanupProgressTables();
    }

    private void prepareData() {
        uploadBaseSourceFile(baseSourceAccMaster.getSourceName(), "AccountMaster206", baseSourceVersion);
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("WeeklyHdfsToS3Publish");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourcesStep1 = new ArrayList<String>();
            baseSourcesStep1.add(baseSourceAccMaster.getSourceName());
            step1.setBaseSources(baseSourcesStep1);
            step1.setTransformer(SourceToS3Publisher.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());

            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException("Transformation Configuration create fail:" + e.getMessage());
        }
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
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // TODO Auto-generated method stub

    }

}

