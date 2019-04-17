package com.latticeengines.datacloud.etl.transformation.service.impl.source;

import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.S3PathBuilder;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.source.ConsolidateCollectionTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateCollectionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ConsolidateCollectionTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateCollectionTestNG.class);

    private static final String VENDOR_NAME = "BUILTWITH";
    private static final String INGESTION_NAME = "BuiltWithRaw";

    @Inject
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Override
    protected String getTargetSourceName() {
        return "TestBWConsolidated";
    }

    @Test(groups = "pipeline2")
    public void testTransformation() {
        log.info("Running test in pod " + HdfsPodContext.getHdfsPodId());
        prepareTestDataInS3();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("ConsolidateCollectionTestNG");
            configuration.setVersion(targetVersion);
            // -----------
            TransformationStepConfig consolidate = consolidate();
            // -----------
            List<TransformationStepConfig> steps = Collections.singletonList(consolidate);
            // -----------
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // correctness is tested in the dataflow functional test
        log.info("Start to verify records one by one.");
        int rowCount = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            //System.out.println(record);
            rowCount++;
        }
        // Assert.assertEquals(rowCount, 999);
    }

    private TransformationStepConfig consolidate() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(ConsolidateCollectionTransformer.TRANSFORMER_NAME);
        ConsolidateCollectionConfig coreConfig = new ConsolidateCollectionConfig();
        coreConfig.setVendor(VENDOR_NAME);
        coreConfig.setRawIngestion(INGESTION_NAME);
        step.setConfiguration(JsonUtils.serialize(coreConfig));
        step.setTargetSource(getTargetSourceName());
        return step;
    }

    private void prepareTestDataInS3() {
        String ingestionDir = S3PathBuilder.constructIngestionDir(INGESTION_NAME).toS3Key();
        s3Service.cleanupPrefix(s3Bucket, ingestionDir);
        uploadIngestedAvroToS3("2018-11-10_18-00-00_UTC.avro", ingestionDir);
        uploadIngestedAvroToS3("2018-11-11_23-00-00_UTC.avro", ingestionDir);
    }

    private void uploadIngestedAvroToS3(String avroFile, String s3Prefix) {
        String resource = "ingestions/" + INGESTION_NAME + "/" + avroFile;
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        String s3Key = s3Prefix + "/" + avroFile;
        s3Service.uploadInputStream(s3Bucket, s3Key, is, true);
    }


}
