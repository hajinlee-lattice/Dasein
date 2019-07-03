package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.publish.SourceToS3Publisher;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceIngestion;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

@Component
public class SourceToS3PublisherTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SourceToS3PublisherTestNG.class);

    // General test
    private GeneralSource baseSrc1 = new GeneralSource("TestSource1");
    // Already exists on S3, test re-publish
    private GeneralSource baseSrc2 = new GeneralSource("TestSource2");
    // Source without schema directory
    private GeneralSource baseSrc3 = new GeneralSource("TestSource3");
    // Source with multiple files in snapshot folder
    private GeneralSource baseSrc4 = new GeneralSource("TestSource4");

    // Sources for multi-source step
    private IngestionSource baseSrc5 = new IngestionSource("TestSource5");///
    private GeneralSource baseSrc6 = new GeneralSource("TestSource6");///

    // Place holder of target source whose name is used as pod
    private GeneralSource source = new GeneralSource(
            SourceToS3PublisherTestNG.class.getSimpleName() + UUID.randomUUID().toString());

    // Sources with schema directory
    private Set<String> expectedSrcWithSchema = ImmutableSet.of( //
            baseSrc1.getSourceName(), //
            baseSrc2.getSourceName(), //
            baseSrc4.getSourceName(), //
            baseSrc6.getSourceName());

    // Base source name -> expected snapshot file names -- initialized in
    // initExpectedSnapshotFiles()
    private Map<String, List<String>> expectedSnapshotFiles;

    @Inject
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    private String basedSourceVersion = "2019-06-25_19-01-34_UTC";

    private String laterSourceVersion = "2019-06-29_21-05-11_UTC";

    private String earlySourceVersion = "2019-06-22_17-07-43_UTC";

    @Test(groups = "pipeline1")
    public void testTransformation() throws IOException {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        verifyPublishExistS3(progress);
        cleanupProgressTables();
    }

    @AfterClass(groups = "pipeline1", enabled = false)
    private void destroy() {
        cleanup();
    }

    /****************************************
     * Construct pipeline job configuration
     ****************************************/

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("HdfsToS3Publish");
            configuration.setVersion(targetVersion);

            List<String> baseSources = new ArrayList<>();
            baseSources.add(baseSrc5.getSourceName());
            baseSources.add(baseSrc6.getSourceName());

            List<TransformationStepConfig> steps = new ArrayList<>();

            TransformationStepConfig step1 = createStep(baseSrc1);
            TransformationStepConfig step2 = createStep(baseSrc2);
            TransformationStepConfig step3 = createStep(baseSrc3);
            TransformationStepConfig step4 = createStep(baseSrc4);
            TransformationStepConfig step5 = createStep(baseSrc5);

            step5.setBaseSources(baseSources);
            step5.setBaseIngestions(Collections.singletonMap(baseSrc5.getSourceName(),
                    new SourceIngestion(baseSrc5.getIngestionName())));

            steps.add(step1);
            steps.add(step2);
            steps.add(step3);
            steps.add(step4);
            steps.add(step5);

            configuration.setSteps(steps);

            return configuration;
        } catch (Exception e) {
            throw new RuntimeException("Transformation Configuration create failed", e);
        }
    }

    private TransformationStepConfig createStep(Source baseSource) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Collections.singletonList(baseSource.getSourceName()));
        step.setTransformer(SourceToS3Publisher.TRANSFORMER_NAME);
        step.setTargetSource(source.getSourceName());
        return step;
    }

    /*******************
     * Initialization
     *******************/

    private void prepareData() throws IOException {
        initExpectedSnapshotFiles();


        s3FilePrepare();

        uploadBaseSourceFile(baseSrc1, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceFile(baseSrc2, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceFile(baseSrc3, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceDir(baseSrc4.getSourceName(), SourceToS3PublisherTestNG.class.getSimpleName(),
                basedSourceVersion);
        uploadBaseSourceFile(baseSrc5, "AccountMaster206.avro", basedSourceVersion);
        uploadBaseSourceFile(baseSrc6, "AccountTable1", basedSourceVersion);
        System.out.println("BaseSrc5 getName: " + baseSrc5.getSourceName());

        createSchema(baseSrc1, basedSourceVersion);
        createSchema(baseSrc2, basedSourceVersion);
        createSchema(baseSrc4, basedSourceVersion);
        createSchema(baseSrc6, basedSourceVersion);
    }

    private void createSchema(Source baseSource, String version) {
        try {
            extractSchema(baseSource, version, hdfsPathBuilder
                    .constructSnapshotDir(baseSource.getSourceName(), version).toString());
        } catch (Exception e) {
            log.error("Fail to extract schema for source {} at version {}", baseSource.getSourceName(),
                    version);
            throw new RuntimeException(e);
        }
    }

    private void s3FilePrepare() throws IOException {
        currentVerionPrepare(baseSrc1, earlySourceVersion);
        currentVerionPrepare(baseSrc2, basedSourceVersion);
        currentVerionPrepare(baseSrc5, laterSourceVersion);

        String resource = "sources/" + "AccountTable1.avro";
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        String s3Key = hdfsPathBuilder.constructSnapshotDir(baseSrc2.getSourceName(), basedSourceVersion) + "/"
                + "AccountTable1.avro";
        s3Service.uploadInputStream(s3Bucket, s3Key, inputStream, true);

        System.out.println("s3FilePrepare finished!!");
    }

    private void currentVerionPrepare(Source baseSource, String sourceVerion) throws IOException {
        InputStream inputStream = IOUtils.toInputStream(sourceVerion, "UTF-8");
        String path = hdfsPathBuilder.constructTransformationSourceDir(baseSource, sourceVerion) + "/";
        String s3Key;
        if (baseSource instanceof IngestionSource) {
            s3Key = path.substring(0, path.lastIndexOf(((IngestionSource) baseSource).getIngestionName()) + 12)
                    + hdfsPathBuilder.VERSION_FILE;
        } else {
            s3Key = path.substring(0, path.lastIndexOf(baseSource.getSourceName()) + 12)
                    + hdfsPathBuilder.VERSION_FILE;
        }
        // System.out.println("s3Key: " + s3Key);
        s3Service.uploadInputStream(s3Bucket, s3Key, inputStream, true);
    }


    private void initExpectedSnapshotFiles() {
        expectedSnapshotFiles = ImmutableMap.of(//
                baseSrc1.getSourceName(), Arrays.asList("AccountMaster206.avro"), //
                baseSrc2.getSourceName(), Arrays.asList("AccountMaster206.avro"), //
                baseSrc3.getSourceName(), Arrays.asList("AccountMaster206.avro"), //
                baseSrc4.getSourceName(), Arrays.asList("part-0000.avro", "part-0001.avro"), //
                baseSrc6.getSourceName(), Arrays.asList("AccountTable1.avro") //
        );
    }

    /*****************
     * Verification
     *****************/

    private void verifyPublishExistS3(TransformationProgress progress) {
        stepSuccessValidate(baseSrc1, basedSourceVersion);
        stepSuccessValidate(baseSrc2, basedSourceVersion);
        stepSuccessValidate(baseSrc3, basedSourceVersion);
        stepSuccessValidate(baseSrc4, basedSourceVersion);
        stepSuccessValidate(baseSrc5, basedSourceVersion);///
        stepSuccessValidate(baseSrc6, basedSourceVersion);///
    }

    private void stepSuccessValidate(Source baseSource, String version) {
        String sourceName = baseSource.getSourceName();
        try {
            log.info("Checking the objects of Source: {}", sourceName);

            String versionFilePath;

            if (!baseSource.getSourceName().contains("Ingestion")) {

                // Verify snapshot files
                List<String> snapshotFiles = getExpectedSnapshotFiles(baseSource);
                validateCopySucseess(snapshotFiles);

                versionFilePath = hdfsPathBuilder.constructVersionFile(sourceName).toString();
            } else {
                String ingestionName = baseSource.getSourceName()
                        .substring(baseSource.getSourceName().lastIndexOf("_") + 1);
                Path ingestionPath = hdfsPathBuilder.constructIngestionDir(ingestionName);
                List<String> IngestionVerFiles = new ArrayList<>();
                IngestionVerFiles
                        .add(ingestionPath.append(basedSourceVersion).append("AccountMaster206.avro").toString());
                IngestionVerFiles
                        .add(ingestionPath.append(basedSourceVersion).append(HdfsPathBuilder.SUCCESS_FILE).toString());
                validateCopySucseess(IngestionVerFiles);

                versionFilePath = ingestionPath.append(HdfsPathBuilder.VERSION_FILE).toString();
            }

            // Verify schema file
            if (expectedSrcWithSchema.contains(sourceName)) {
                String schemaFile = hdfsPathBuilder.constructSchemaDir(sourceName, version)
                        .append(sourceName + ".avsc").toString();
                validateCopySucseess(Arrays.asList(schemaFile));
            }

            // Verify current version file

            validateCopySucseess(Arrays.asList(versionFilePath));
        } catch (Exception e) {
            log.error("Fail to validate publising source {} at version {}", sourceName, version);
            throw new RuntimeException(e);
        }
    }

    private List<String> getExpectedSnapshotFiles(Source baseSource) {
        Path snapshotPath = hdfsPathBuilder.constructTransformationSourceDir(baseSource, basedSourceVersion);
        List<String> expectedFiles = expectedSnapshotFiles.get(baseSource.getSourceName());
        Assert.assertTrue(CollectionUtils.isNotEmpty(expectedFiles));

        List<String> lists = expectedFiles.stream() //
                .map(file -> snapshotPath.append(file).toString()).collect(Collectors.toList());
        lists.add(snapshotPath.append(HdfsPathBuilder.SUCCESS_FILE).toString());
        return lists;
    }

    private void validateCopySucseess(List<String> files) throws IOException {
        files.forEach(file -> {
            Assert.assertTrue(s3Service.objectExist(s3Bucket, file));
        });
    }

    /*****************
     * Final cleanup
     *****************/

    private void cleanup() {
        String podDir = hdfsPathBuilder.podDir().toString();
        try {
            cleanupS3Path(podDir);
            HdfsUtils.rmdir(yarnConfiguration, podDir);
        } catch (IOException e) {
            throw new RuntimeException("Fail to clean up pod " + podDir, e);
        }
    }

    private void cleanupS3Path(String path) throws IOException {
        if (s3Service.isNonEmptyDirectory(s3Bucket, path)) {
            s3Service.cleanupPrefix(s3Bucket, path);
        }
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
