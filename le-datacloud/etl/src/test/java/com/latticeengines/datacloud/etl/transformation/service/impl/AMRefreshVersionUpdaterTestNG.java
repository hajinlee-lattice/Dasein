package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AMRefreshVersionUpdater;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMRefreshVersionUpdaterTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private Long refreshVerValBefore;
    @Autowired
    private DataCloudVersionService dataCloudVersionService;
    GeneralSource source = new GeneralSource("LDCDEV_AMRefreshVersionUpdater");
    GeneralSource baseSource = new GeneralSource("AMRefreshVersionUpdater");

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        refreshVerValBefore = Long.valueOf(dataCloudVersionService.currentApprovedVersion().getRefreshVersionVersion());
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    // This data is not useful. It is used to fake an AVRO file
    private void prepareData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        Object[][] data = new Object[][] { { "kaggle.com", "123456789" } };

        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
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
        Long refreshVerValAfter = Long
                .valueOf(dataCloudVersionService.currentApprovedVersion().getRefreshVersionVersion());
        // To verify if the new timestamp is greater i.e it refreshed
        Assert.assertTrue(refreshVerValAfter > refreshVerValBefore);
    }

}
