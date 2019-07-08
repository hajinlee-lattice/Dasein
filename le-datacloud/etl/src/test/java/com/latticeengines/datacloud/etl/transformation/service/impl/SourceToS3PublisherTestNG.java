package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
    private IngestionSource baseSrc5 = new IngestionSource("TestSource5");
    private GeneralSource baseSrc6 = new GeneralSource("TestSource6");

    // Place holder of target source whose name is used as pod
    private GeneralSource source = new GeneralSource(
            SourceToS3PublisherTestNG.class.getSimpleName() + UUID.randomUUID().toString());

    private String basedSourceVersion = "2019-06-25_19-01-34_UTC";

    // Test for _CURRENT_VERSION file update
    private String laterSourceVersion = "2019-06-29_21-05-11_UTC";

    private String earlySourceVersion = "2019-06-22_17-07-43_UTC";
    @Inject
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Test(groups = "pipeline1")
    public void testTransformation() throws IOException {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        verifyPublishExistS3();
        cleanupProgressTables();
    }

    @AfterClass(groups = "pipeline1", enabled = true)
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

            List<TransformationStepConfig> steps = new ArrayList<>();

            TransformationStepConfig step1 = createStep(Collections.singletonList(baseSrc1));
            TransformationStepConfig step2 = createStep(Collections.singletonList(baseSrc2));
            TransformationStepConfig step3 = createStep(Collections.singletonList(baseSrc3));
            TransformationStepConfig step4 = createStep(Collections.singletonList(baseSrc4));
            TransformationStepConfig step5 = createStep(Arrays.asList(baseSrc5, baseSrc6));

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

    private TransformationStepConfig createStep(List<Source>  baseSources) {
        List<String> baseSourceNames = baseSources.stream().map(baseSource -> baseSource.getSourceName())
                .collect(Collectors.toList());

        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(baseSourceNames);
        step.setTransformer(SourceToS3Publisher.TRANSFORMER_NAME);
        step.setTargetSource(source.getSourceName());

        for (Source source : baseSources) {
            if (source instanceof IngestionSource) {
                step.setBaseIngestions(Collections.singletonMap(((IngestionSource) source).getSourceName(),
                        new SourceIngestion(((IngestionSource) source).getIngestionName())));
            }
        }

        return step;
    }

    /*******************
     * Initialization
     *******************/

    // Sources with schema directory
    private Set<String> expectedSrcWithSchema = ImmutableSet.of( //
            baseSrc1.getSourceName(), //
            baseSrc2.getSourceName(), //
            baseSrc4.getSourceName(), //
            baseSrc6.getSourceName());

    // Base source -> expected snapshot file names
    @SuppressWarnings("serial")
    private Map<Source, List<String>> expectedDataFiles = new HashMap<Source, List<String>>() {
        {
            put(baseSrc1, Arrays.asList("AccountMaster206.avro"));
            put(baseSrc2, Arrays.asList("AccountMaster206.avro"));
            put(baseSrc3, Arrays.asList("AccountMaster206.avro"));
            put(baseSrc4, Arrays.asList("part-0000.avro", "part-0001.avro"));
            put(baseSrc5, Arrays.asList("AccountMaster206.avro"));
            put(baseSrc6, Arrays.asList("AccountTable1.avro"));
        }
    };

    // Base source -> expected _CURRENT_VERSION
    @SuppressWarnings("serial")
    private Map<Source, String> expectedDates = new HashMap<Source, String>() {
        {
            put(baseSrc1, basedSourceVersion);
            put(baseSrc2, basedSourceVersion);
            put(baseSrc3, basedSourceVersion);
            put(baseSrc4, basedSourceVersion);
            put(baseSrc5, laterSourceVersion);
            put(baseSrc6, basedSourceVersion);
        }
    };

    private void prepareData() throws IOException {
        s3FilePrepare();

        uploadBaseSourceFile(baseSrc1, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceFile(baseSrc2, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceFile(baseSrc3, "AccountMaster206", basedSourceVersion);
        uploadBaseSourceDir(baseSrc4.getSourceName(), SourceToS3PublisherTestNG.class.getSimpleName(),
                basedSourceVersion);
        uploadBaseSourceFile(baseSrc5, "AccountMaster206.avro", basedSourceVersion);
        uploadBaseSourceFile(baseSrc6, "AccountTable1", basedSourceVersion);

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
        // Prepare _CURRENT_VERION file
        uploadToS3(baseSrc1, earlySourceVersion, false);
        uploadToS3(baseSrc2, basedSourceVersion, false);
        uploadToS3(baseSrc5, laterSourceVersion, false);

        // Prepare data file
        uploadToS3(baseSrc2, basedSourceVersion, true);
    }

    private void uploadToS3(Source baseSource, String sourceVerion, boolean isDataDir) throws IOException {

        InputStream inputStream = isDataDir
                ? Thread.currentThread().getContextClassLoader().getResourceAsStream("sources/" + "AccountTable1.avro")
                : IOUtils.toInputStream(sourceVerion, "UTF-8");

        String s3Key = isDataDir
                ? hdfsPathBuilder.constructTransformationSourceDir(baseSource, sourceVerion)//
                        + "/" + "AccountTable1.avro"
                : hdfsPathBuilder.constructVersionFile(baseSource).toString();

        s3Service.uploadInputStream(s3Bucket, s3Key, inputStream, true);
    }

    
   
    /*****************
     * Verification
     *****************/

    private void verifyPublishExistS3() {
        stepSuccessValidate(baseSrc1, basedSourceVersion);
        stepSuccessValidate(baseSrc2, basedSourceVersion);
        stepSuccessValidate(baseSrc3, basedSourceVersion);
        stepSuccessValidate(baseSrc4, basedSourceVersion);
        stepSuccessValidate(baseSrc5, basedSourceVersion);
        stepSuccessValidate(baseSrc6, basedSourceVersion);
    }



    
    private void stepSuccessValidate(Source baseSource, String version) {
        String sourceName = baseSource.getSourceName();
        try {
            log.info("Checking the objects of Source: {}", sourceName);

            // Verify data files
            List<String> dataFiles = getExpectedDataFiles(baseSource);
            validateCopySuccess(dataFiles);

            // Verify schema file
            if (expectedSrcWithSchema.contains(sourceName)) {
                String schemaFile = hdfsPathBuilder.constructSchemaDir(sourceName, version)
                        .append(sourceName + ".avsc").toString();
                validateCopySuccess(Arrays.asList(schemaFile));

            }

            // Verify current version file
            String versionFilePath = hdfsPathBuilder.constructVersionFile(baseSource).toString();
            validateCopySuccess(Arrays.asList(versionFilePath));
            isUpdatedDate(baseSource, versionFilePath);
           
        } catch (Exception e) {
            log.error("Fail to validate publising source {} at version {}", sourceName, version);
            throw new RuntimeException(e);
        }
    }
    
    private void isUpdatedDate(Source baseSource, String versionFilePath) throws ParseException, IOException {
        Date hdfsDate = HdfsPathBuilder.dateFormat.parse(expectedDates.get(baseSource));

        @SuppressWarnings("deprecation")
        Date s3Date = HdfsPathBuilder.dateFormat
                .parse(IOUtils.toString(s3Service.readObjectAsStream(s3Bucket, versionFilePath)));
        Assert.assertEquals(hdfsDate, s3Date);
    }

    private List<String> getExpectedDataFiles(Source baseSource) {
        Path dataPath = hdfsPathBuilder.constructTransformationSourceDir(baseSource, basedSourceVersion);
        List<String> expectedFiles = expectedDataFiles.get(baseSource);
        Assert.assertTrue(CollectionUtils.isNotEmpty(expectedFiles));

        List<String> lists = expectedFiles.stream() //
                .map(file -> dataPath.append(file).toString()).collect(Collectors.toList());
        lists.add(dataPath.append(HdfsPathBuilder.SUCCESS_FILE).toString());
        return lists;
    }

    private void validateCopySuccess(List<String> files) throws IOException {
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
