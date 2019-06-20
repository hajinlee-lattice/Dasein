package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.publish.SourceToS3Publisher;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceToS3PublisherDeploymentTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SourceToS3PublisherDeploymentTestNG.class);

    GeneralSource baseSourceAccMaster = new GeneralSource("AccountMasterVerify");
    GeneralSource source = new GeneralSource("LDCDEV_SourceToS3Publisher");

    @Autowired
    private S3Service s3Service;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;


    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        Assert.assertTrue(s3VerifyPublishExist(progress));
        cleanupProgressTables();
    }

    private void prepareData() {
        uploadBaseSourceFile(baseSourceAccMaster.getSourceName(), "AccountMaster206", baseSourceVersion);
        try {
            extractSchema(baseSourceAccMaster, baseSourceVersion, hdfsPathBuilder
                    .constructSnapshotDir(baseSourceAccMaster.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s",
                    baseSourceAccMaster.getSourceName(), baseSourceVersion));
        }
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
        return hdfsPathBuilder.constructSnapshotDir(baseSourceAccMaster.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(baseSourceAccMaster.getSourceName(), baseSourceVersion).toString();
    }

    protected String getSchemaPathForResult() {
        return hdfsPathBuilder.constructSchemaDir(baseSourceAccMaster.getSourceName(), baseSourceVersion).toString();
    }

    protected String getVerFilePathForResult() {
        Source targetSource = sourceService.findBySourceName(baseSourceAccMaster.getSourceName());
        return hdfsPathBuilder.constructVersionFile(targetSource).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // TODO Auto-generated method stub

    }

    private boolean s3VerifyPublishExist(TransformationProgress progress) {
        String s3SnapshotPath = getPathForResult();
        String s3SchemaPath = getSchemaPathForResult();
        String s3VersionFilePath = getVerFilePathForResult();

        return checkS3Objects(s3SnapshotPath, s3SchemaPath, s3VersionFilePath);
    }

    private boolean checkS3Objects(String s3SnapshotPath, String s3SchemaPath, String s3VersionFilePath) {
        try {
            checkS3PrefixObjExist(s3SnapshotPath);
            checkS3PrefixObjExist(s3SchemaPath);
            checkS3PrefixObjExist(s3VersionFilePath);
            return true;
        } catch (Exception e) {
            throw new RuntimeException("S3 Publish fail, missing objects:" + e.getMessage());
        }
    }

    private boolean checkS3PrefixObjExist(String Prefix) {
        System.out.println("Checking the objects of Prefix:" + Prefix);
        if (s3Service.isNonEmptyDirectory(s3Bucket, Prefix)) {
            List<S3ObjectSummary> Obejects = s3Service.listObjects(s3Bucket, Prefix);
            for (S3ObjectSummary object : Obejects) {
                if (!s3Service.objectExist(s3Bucket, object.getKey())) {
                    throw new RuntimeException();
                }
            }
        } else {
            throw new RuntimeException();
        }
        return true;
    }


}

