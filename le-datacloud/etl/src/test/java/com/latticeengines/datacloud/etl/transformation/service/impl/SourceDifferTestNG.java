package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.Diff;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DifferConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceDifferTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(SourceDifferTestNG.class);

    private GeneralSource src1 = new GeneralSource("SRC1");
    private GeneralSource src2 = new GeneralSource("SRC2");
    private GeneralSource src3 = new GeneralSource("SRC3");
    private GeneralSource src4 = new GeneralSource("SRC4");
    private GeneralSource source = new GeneralSource("AMDiff");
    private GeneralSource source0 = new GeneralSource("AMDiff0");
    private GeneralSource source1 = new GeneralSource("AMDiff1");

    @Test(groups = "functional")
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(source0, targetVersion);
        confirmIntermediateSource(source1, targetVersion);
        confirmIntermediateSource(source, targetVersion);
        cleanupProgressTables();
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
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("AMDiff");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(src1.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(Diff.TRANSFORMER_NAME);
        step0.setConfiguration(getDifferConfigWithVersionSet());
        step0.setTargetSource(source0.getSourceName());

        // ----------
        TransformationStepConfig step1 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(src2.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(Diff.TRANSFORMER_NAME);
        step1.setConfiguration(getDifferConfigNoVersionSet());
        step1.setTargetSource(source1.getSourceName());

        // ----------
        TransformationStepConfig step2 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(src4.getSourceName());
        baseSources.add(src3.getSourceName());
        step2.setBaseSources(baseSources);
        step2.setTransformer(Diff.TRANSFORMER_NAME);
        step2.setConfiguration(getDifferConfigTwoSources());
        step2.setTargetSource(source.getSourceName());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);
        configuration.setKeepTemp(true);
        return configuration;
    }

    private String getDifferConfigWithVersionSet() {
        DifferConfig config = new DifferConfig();
        config.setDiffVersion("2017-08-01_00-00-00_UTC");
        config.setDiffVersionCompared("2017-07-01_00-00-00_UTC");
        String[] keys = { "ID" };
        config.setKeys(keys);
        String[] excludeFields = { "Attr5" };
        config.setExcludeFields(excludeFields);
        return JsonUtils.serialize(config);
    }

    private String getDifferConfigNoVersionSet() {
        DifferConfig config = new DifferConfig();
        String[] keys = { "ID" };
        config.setKeys(keys);
        String[] excludeFields = { "Attr5" };
        config.setExcludeFields(excludeFields);
        return JsonUtils.serialize(config);
    }

    private String getDifferConfigTwoSources() {
        DifferConfig config = new DifferConfig();
        String[] keys = { "ID" };
        config.setKeys(keys);
        String[] excludeFields = { "Attr5" };
        config.setExcludeFields(excludeFields);
        return JsonUtils.serialize(config);
    }

    private Object[][] dataCompared = new Object[][] { //
            { 0L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 1L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 2L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 3L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 4L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 5L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 6L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 7L, 1, 1.1F, 1.2, "AAA", "111" }, //
    };

    private Object[][] data = new Object[][] { //
            { 1L, 1, 1.1F, 1.2, "AAA", "111" }, //
            { 2L, 2, 1.1F, 1.2, "AAA", "111" }, //
            { 3L, 1, 2.1F, 1.2, "AAA", "111" }, //
            { 4L, 1, 1.1F, 2.2, "AAA", "111" }, //
            { 5L, 1, 1.1F, 1.2, "BBB", "111" }, //
            { 6L, 1, 1.1F, null, "AAA", "111" }, //
            { 7L, 1, 1.1F, 1.2, "AAA", "222" }, //
            { 20L, 1, 1.1F, 1.2, "AAA", "111" }, //
    };

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("ID", Long.class));
        schema.add(Pair.of("Attr1", Integer.class));
        schema.add(Pair.of("Attr2", Float.class));
        schema.add(Pair.of("Attr3", Double.class));
        schema.add(Pair.of("Attr4", String.class));
        schema.add(Pair.of("Attr5", String.class));

        uploadBaseSourceData(src1.getSourceName(), "2017-07-01_00-00-00_UTC", schema, dataCompared);
        uploadBaseSourceData(src1.getSourceName(), "2017-08-01_00-00-00_UTC", schema, data);

        uploadBaseSourceData(src2.getSourceName(), "2017-07-01_00-00-00_UTC", schema, dataCompared);
        uploadBaseSourceData(src2.getSourceName(), "2017-08-01_00-00-00_UTC", schema, data);

        uploadBaseSourceData(src3.getSourceName(), baseSourceVersion, schema, dataCompared);
        uploadBaseSourceData(src4.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info("Start to verify records in source " + source);
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            count++;
        }
        Assert.assertEquals(6, count);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {

    }
}
