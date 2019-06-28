package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
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

    private GeneralSource[] sourceWithSchema = { baseSrc1, baseSrc2, baseSrc4 };

    private Map<String, List<String>> expectedSchemaFiles;
    private Map<String, List<String>> expectedSnapshotFiles;

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
        prepareSchemaExpectedFiles();
        prepareSnapshotExpectedFiles();

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

    private String getSchemaFilePath(Source baseSource) {
        return hdfsPathBuilder.constructSchemaDir(baseSource.getSourceName(), basedSourceVersion).toString() + "/"
                + baseSource.getSourceName() + ".avsc";
    }

    private List<String> getSnapshotFiles(Source baseSource, String... files) {
        String SnapshotPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), basedSourceVersion)
                .toString();
        List<String> lists = Arrays.stream(files)//
                .map(file -> SnapshotPath + "/"
                        + file)
                .collect(Collectors.toList());
        lists.add(SnapshotPath + "/" + "_SUCCESS");
        return lists;
    }

    private void prepareSchemaExpectedFiles() {
        expectedSchemaFiles = Arrays.stream(sourceWithSchema) //
                .collect(Collectors.toMap(baseSrc -> baseSrc.getSourceName(),
                        baseSrc -> Collections.singletonList(getSchemaFilePath(baseSrc))));
    }

    private void prepareSnapshotExpectedFiles() {
        expectedSnapshotFiles = ImmutableMap.of(//
                baseSrc1.getSourceName(), getSnapshotFiles(baseSrc1, "AccountMaster206.avro"), //
                baseSrc2.getSourceName(), getSnapshotFiles(baseSrc2, "AccountMaster206.avro"), //
                baseSrc3.getSourceName(), getSnapshotFiles(baseSrc3, "AccountMaster206.avro"), //
                baseSrc4.getSourceName(), getSnapshotFiles(baseSrc4, "part-0000.avro", "part-0001.avro") //
        );
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

    private void cleanupS3Path(String hdfsDir) throws IOException {
        if (s3Service.isNonEmptyDirectory(s3Bucket, hdfsDir)) {
            s3Service.cleanupPrefix(s3Bucket, hdfsDir);
        }
    }

    private void cleanupSourcePathsS3(Source baseSource, String baseSourceVersion) throws IOException {
        String s3SnapshotPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion)
                .toString();
        String s3SchemaPath = hdfsPathBuilder.constructSchemaDir(baseSource.getSourceName(), baseSourceVersion)
                .toString();
        String s3VersionFilePath = hdfsPathBuilder.constructVersionFile(baseSource.getSourceName()).toString();
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
            String sourceName = baseSource.getSourceName();

            log.info("Checking the objects of Source: {} ", sourceName);

            String s3SchemaPath = hdfsPathBuilder.constructSchemaDir(baseSource.getSourceName(), baseSourceVersion)
                    .toString();
            String s3VersionFilePath = hdfsPathBuilder.constructVersionFile(baseSource.getSourceName()).toString();

            validateCopySucseess(getSnapshotFileList(sourceName));
            if (HdfsUtils.fileExists(yarnConfiguration, s3SchemaPath)) {
                validateCopySucseess(getSchemaFileList(sourceName));
            }
            validateCopySucseess(getCurrentVerFileList(s3VersionFilePath));
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void validateCopySucseess(List<String> files) throws IOException {
        for (String key : files) {
            log.info("Check key : {} ", key);
            if (!s3Service.objectExist(s3Bucket, key)) {
                throw new RuntimeException("File not Exist in S3:" + key);
            }
        }
    }

    private List<String> getCurrentVerFileList(String Prefix) {
        return Collections.singletonList(Prefix);
    }

    private List<String> getSchemaFileList(String sourceName) {
        return expectedSchemaFiles.get(sourceName);
    }

    private List<String> getSnapshotFileList(String sourceName) {
        return expectedSnapshotFiles.get(sourceName);
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
    protected String getTargetSourceName() {
        return null;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}


