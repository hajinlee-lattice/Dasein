package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.publish.SourceToS3Publisher;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceToS3PublisherTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SourceToS3PublisherTestNG.class);

    private GeneralSource baseSrc1 = new GeneralSource("TestSource1");
    // S3 already exist
    private GeneralSource baseSrc2 = new GeneralSource("TestSource2");
    // no schema
    private GeneralSource baseSrc3 = new GeneralSource("TestSource3");
    // multiple files in Snapshot
    private GeneralSource baseSrc4 = new GeneralSource("TestSource4");

    private String sourceName;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    private String basedSourceVersion = "2019-06-25_19-01-34_UTC";

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() throws IOException {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        verifyPublishExistS3(progress);
        cleanupProgressTables();
    }


    private void prepareData() throws IOException {
        s3FilePrepare();
        uploadBaseSourceFile(baseSrc1, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceFile(baseSrc2, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceFile(baseSrc3, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceDir(baseSrc4.getSourceName(), "TestSource", basedSourceVersion);


        createSchema(baseSrc1, basedSourceVersion);
        createSchema(baseSrc2, basedSourceVersion);
        createSchema(baseSrc4, basedSourceVersion);
    }

    private void s3FilePrepare() {
        String resource = "sources/" + "AccountTable1.avro";
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        String s3Key = hdfsPathBuilder.constructSnapshotDir(baseSrc2.getSourceName(), basedSourceVersion)
                + "AccountTable1.avro";
        s3Service.uploadInputStream(s3Bucket, s3Key, inputStream, true);
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("WeeklyHdfsToS3Publish");
            configuration.setVersion(targetVersion);

            List<TransformationStepConfig> steps = new ArrayList<>();

            TransformationStepConfig step1 = createStep(baseSrc1);
            TransformationStepConfig step2 = createStep(baseSrc2);
            TransformationStepConfig step3 = createStep(baseSrc3);
            TransformationStepConfig step4 = createStep(baseSrc4);

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



    private void cleanupS3Path(String hdfsDir) throws IOException {
        if (s3Service.isNonEmptyDirectory(s3Bucket, hdfsDir)) {
            s3Service.cleanupPrefix(s3Bucket, hdfsDir);
        }
    }

    private void cleanupSourcePathsS3(Source baseSource, String baseSourceVersion) throws IOException {
        String s3SnapshotPath = getSnapshotPathForResult(baseSource, baseSourceVersion);
        String s3SchemaPath = getSchemaPathForResult(baseSource, baseSourceVersion);
        String s3VersionFilePath = getVerFilePathForResult(baseSource, baseSourceVersion);
        cleanupS3Path(s3SnapshotPath);
        cleanupS3Path(s3SchemaPath);
        cleanupS3Path(s3VersionFilePath);
    }

    private void verifyPublishExistS3(TransformationProgress progress) {
        try {
            Assert.assertTrue(stepSuccessValidate(baseSrc1, basedSourceVersion));
            Assert.assertTrue(stepSuccessValidate(baseSrc2, basedSourceVersion));
            Assert.assertTrue(stepSuccessValidate(baseSrc3, basedSourceVersion));
            Assert.assertTrue(stepSuccessValidate(baseSrc4, basedSourceVersion));
            cleanupSourcePathsS3(baseSrc2, basedSourceVersion);
        } catch (Exception e) {
            throw new RuntimeException("S3 Publish fail:" + e.getMessage());
        }
    }

    private boolean stepSuccessValidate(Source baseSource, String baseSourceVersion) throws IOException {
        try {
            String s3SnapshotPath = getSnapshotPathForResult(baseSource, baseSourceVersion);
            String s3SchemaPath = getSchemaPathForResult(baseSource, baseSourceVersion);
            String s3VersionFilePath = getVerFilePathForResult(baseSource, baseSourceVersion);

            sourceName = baseSource.getSourceName();

            validateCopySucseess(s3SnapshotPath, "Snapshot");
            if (HdfsUtils.fileExists(yarnConfiguration, s3SchemaPath)) {
                validateCopySucseess(s3SchemaPath, "Schema");
            }
            validateCopySucseess(s3VersionFilePath, "_CURRENT_VERSION");
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void validateCopySucseess(String Prefix, String dir) throws IOException {
        log.info("Checking the objects of Prefix: {}", Prefix);

        List<String> files = new ArrayList<>();
        if (StringUtils.equals(dir, "_CURRENT_VERSION")) {
            files = getCurrentVerFileList(files, Prefix);
        }
        if (StringUtils.equals(dir, "Schema")) {
            files = getSchemaFileList(files, Prefix);
        }
        if (StringUtils.equals(dir, "Snapshot")) {
            files = getSnapshotFileList(files, Prefix);
        }
        for (String key : files) {
            log.info("Check key : {} ", key);
            if (!s3Service.objectExist(s3Bucket, key)) {
                throw new RuntimeException("File not Exist in S3:" + key);
            }
        }
    }

    private List<String> getCurrentVerFileList(List<String> files, String Prefix) {
        files.add(Prefix);
        return files;
    }

    private List<String> getSchemaFileList(List<String> files, String prefix) {
        String schema = null;
        switch (sourceName) {
        case "TestSource1":
            schema = prefix + "/TestSource1.avsc";
            break;
        case "TestSource2":
            schema = prefix + "/TestSource2.avsc";
            break;
        case "TestSource4":
            schema = prefix + "/TestSource4.avsc";
            break;
        }
        if (schema != null) {
            files.add(schema);
        }
        return files;
    }

    private List<String> getSnapshotFileList(List<String> files, String prefix) {
        switch (sourceName) {
        case "TestSource4":
            files.add(prefix + "/" + "_SUCCESS");
            files.add(prefix + "/" + "part-0000.avro");
            files.add(prefix + "/" + "part-0001.avro");
            break;
        default:
            files.add(prefix + "/" + "_SUCCESS");
            files.add(prefix + "/" + "AccountMaster206.avro");
        }
        return files;
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
            throw new RuntimeException(e);
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


