package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.publish.SourceToS3Publisher;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceToS3PublisherDeploymentTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SourceToS3PublisherDeploymentTestNG.class);

    private GeneralSource baseSourceAccMaster1 = new GeneralSource("TestSource1");
    // S3 already exist
    private GeneralSource baseSourceAccMaster2 = new GeneralSource("TestSource2");
    // no schema
    private GeneralSource baseSourceAccMaster3 = new GeneralSource("TestSource3");
    // multiple files in Snapshot
    private GeneralSource baseSourceAccMaster4 = new GeneralSource("TestSource4");

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    private String existingSourceVersion = "2019-06-23_18-45-42_UTC";

    @Test(groups = "functional", enabled = true)
    public void testTransformation() throws IOException {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        Assert.assertTrue(VerifyPublishExists3(progress));
        cleanupProgressTables();
    }

    private void prepareData() throws IOException {

        uploadBaseSourceFile(baseSourceAccMaster1.getSourceName(), "AccountMaster206", baseSourceVersion);
        uploadBaseSourceFile(baseSourceAccMaster2.getSourceName(), "AccountMaster206", existingSourceVersion);
        uploadBaseSourceFile(baseSourceAccMaster3.getSourceName(), "AccountMaster206", baseSourceVersion);
        uploadBaseSourceDir(baseSourceAccMaster4.getSourceName(), "", baseSourceVersion);


        createSchema(baseSourceAccMaster1, baseSourceVersion);
        createSchema(baseSourceAccMaster2, existingSourceVersion);
        createSchema(baseSourceAccMaster4, baseSourceVersion);
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("WeeklyHdfsToS3Publish");
            configuration.setVersion(targetVersion);

            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();

            TransformationStepConfig step1 = createStep(baseSourceAccMaster1);
            TransformationStepConfig step2 = createStep(baseSourceAccMaster2);
            TransformationStepConfig step3 = createStep(baseSourceAccMaster3);
            TransformationStepConfig step4 = createStep(baseSourceAccMaster4);

            steps.add(step1);
            steps.add(step2);
            steps.add(step3);
            steps.add(step4);

            configuration.setSteps(steps);

            return configuration;
        } catch (Exception e) {
            throw new RuntimeException("Transformation Configuration create fail:" + e.getMessage());
        }
    }

    @Override
    protected String getPathForResult() {
        return hdfsPathBuilder.constructSnapshotDir(baseSourceAccMaster1.getSourceName(), baseSourceVersion).toString();
    }

    protected String getSnapshotPathForResult(Source baseSourceAccMaster, String baseSourceVersion) {
        return hdfsPathBuilder.constructSnapshotDir(baseSourceAccMaster.getSourceName(), baseSourceVersion).toString();
    }

    protected String getSchemaPathForResult(Source baseSourceAccMaster, String baseSourceVersion) {
        return hdfsPathBuilder.constructSchemaDir(baseSourceAccMaster.getSourceName(), baseSourceVersion).toString();
    }

    protected String getVerFilePathForResult(Source baseSourceAccMaster, String baseSourceVersion) {
        Source targetSource = sourceService.findBySourceName(baseSourceAccMaster.getSourceName());
        return hdfsPathBuilder.constructVersionFile(targetSource).toString();
    }



    private boolean VerifyPublishExists3(TransformationProgress progress) {
        try {
            Source baseSource1 = sourceService.findBySourceName(baseSourceAccMaster1.getSourceName());
            Source baseSource2 = sourceService.findBySourceName(baseSourceAccMaster2.getSourceName());
            Source baseSource3 = sourceService.findBySourceName(baseSourceAccMaster3.getSourceName());
            Source baseSource4 = sourceService.findBySourceName(baseSourceAccMaster3.getSourceName());

            stepSuccessValidate(baseSource1, baseSourceVersion);
            stepSuccessValidate(baseSource2, existingSourceVersion);
            stepSuccessValidate(baseSource3, baseSourceVersion);
            stepSuccessValidate(baseSource4, baseSourceVersion);

            return true;
        } catch (Exception e) {
            throw new RuntimeException("S3 Publish fail:" + e.getMessage());
        }
    }

    private void stepSuccessValidate(Source baseSource, String baseSourceVersion) throws IOException {

        String s3SnapshotPath = getSnapshotPathForResult(baseSource, baseSourceVersion);
        String s3SchemaPath = getSchemaPathForResult(baseSource, baseSourceVersion);
        String s3VersionFilePath = getVerFilePathForResult(baseSource, baseSourceVersion);

        validateCopySucseess(s3SnapshotPath);
        if (HdfsUtils.isDirectory(yarnConfiguration, s3SchemaPath)) {
            validateCopySucseess(s3SchemaPath);
        }
        validateCopySucseess(s3VersionFilePath);
    }

    private void validateCopySucseess(String Prefix) throws IOException {
            System.out.println("Checking the objects of Prefix:" + Prefix);
            String filepath;

            List<String> files = HdfsUtils.onlyGetFilesForDirRecursive(yarnConfiguration, Prefix,
                    (HdfsFileFilter) null, false);
            for (String key : files) {
                filepath = key.substring(key.indexOf(Prefix));
                System.out.println("Key : " + filepath);
                if (!s3Service.objectExist(s3Bucket, filepath)) {
                    throw new RuntimeException("File not Exist in S3:" + filepath);
                }
            }
    }

    private TransformationStepConfig createStep(Source baseSourceAccMaster) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Collections.singletonList(baseSourceAccMaster.getSourceName()));
        step.setTransformer(SourceToS3Publisher.TRANSFORMER_NAME);
        step.setTargetSource(source.getSourceName());
        return step;
    }

    private void createSchema(Source baseSourceAccMaster, String baseSourceVersion) {
        try {
            extractSchema(baseSourceAccMaster, baseSourceVersion, hdfsPathBuilder
                    .constructSnapshotDir(baseSourceAccMaster.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s",
                    baseSourceAccMaster.getSourceName(), baseSourceVersion));
        }
    }

    @Override
    protected void uploadBaseSourceDir(String baseSource, String baseSourceDir, String baseSourceVersion)
            throws IOException {
        String rootDir = "sources/" + baseSourceDir;

        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] sourceResources = resolver.getResources(rootDir + "/*.avro");
        log.info("Resolved resources for " + rootDir);

        int fileIdx = 0;
        for (Resource resource : sourceResources) {
            if (resource.getURI().toString().endsWith(".avro")) {
                InputStream is = resource.getInputStream();
                String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion)
                        .append(String.format("part-%04d.avro", fileIdx)).toString();
                log.info("Upload " + resource.getURI().toString() + " to " + targetPath);
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, targetPath);
                fileIdx++;
            }
        }
        String successPath = hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).append("_SUCCESS")
                .toString();
        InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

    @Override
    protected String getTargetSourceName() {
        return null;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}


