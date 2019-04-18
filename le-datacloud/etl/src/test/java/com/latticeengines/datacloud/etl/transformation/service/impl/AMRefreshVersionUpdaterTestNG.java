package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AMRefreshVersionUpdater;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMRefreshVersionUpdaterTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AMRefreshVersionUpdaterTestNG.class);
    private Long refreshVerValBefore;
    @Autowired
    private DataCloudVersionService dataCloudVersionService;
    GeneralSource source = new GeneralSource("LDCDEV_AMRefreshVersionUpdater");
    GeneralSource baseSource = new GeneralSource("AMRefreshVersionUpdater");
    private int iteration = 0;

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        for (int i = 0; i < 3; i++) { // iterating twice to test if step is
                                      // re-run and source is re-generated if _SUCCESS flag is absent
            iteration = i;
            refreshVerValBefore = Long.valueOf(
                    dataCloudVersionService.currentApprovedVersion().getRefreshVersionVersion());
            prepareData();
            TransformationProgress progress = createNewProgress();
            progress = transformData(progress);
            finish(progress);
            confirmResultFile(progress);
            cleanupProgressTables();
        }
    }

    // This data is not useful. It is used to fake an AVRO file
    private void prepareData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        Object[][] data1 = new Object[][] { { "kaggle.com", "123456789" } };
        Object[][] data2 = new Object[][] { { "google.com", "342141241" } };
        Object[][] data3 = new Object[][] { { "yahoo.com", "111111111" } };
        if (iteration == 0) {
            uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data1);
        }
        if (iteration == 1) {
            uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data2);
        }
        if (iteration == 2) {
            uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data3);
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
        return hdfsPathBuilder
                .constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("WeeklyRefreshVersionUpdate");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourcesStep1 = new ArrayList<String>();
            baseSourcesStep1.add(baseSource.getSourceName());
            step1.setBaseSources(baseSourcesStep1);
            step1.setTransformer(AMRefreshVersionUpdater.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());

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
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Long refreshVerValAfter = Long.valueOf(
                dataCloudVersionService.currentApprovedVersion().getRefreshVersionVersion());
        // To verify if the new timestamp is greater i.e it refreshed
        Assert.assertTrue(refreshVerValAfter > refreshVerValBefore);
        // verify target Source for use case : _SUCCESS flag absent then
        // re-generate that source
        int rowCount = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            if (iteration == 0) {
                Assert.assertTrue(isObjEquals(record.get("Domain"), "kaggle.com"));
                Assert.assertTrue(isObjEquals(record.get("DUNS"), "123456789"));
            }
            if (iteration == 1) {
                Assert.assertTrue(isObjEquals(record.get("Domain"), "kaggle.com"));
                Assert.assertTrue(isObjEquals(record.get("DUNS"), "123456789"));
                String targetSrcPath = getPathForResult() + "/_SUCCESS";
                try {
                    HdfsUtils.rmdir(new Configuration(), targetSrcPath);
                } catch (IOException e) {
                    log.error("Error in deleting provided source path : ", e);
                }
            }
            if (iteration == 2) {
                Assert.assertTrue(isObjEquals(record.get("Domain"), "yahoo.com"));
                Assert.assertTrue(isObjEquals(record.get("DUNS"), "111111111"));
            }
            rowCount++;
        }
        Assert.assertEquals(rowCount, 1);
    }
}
