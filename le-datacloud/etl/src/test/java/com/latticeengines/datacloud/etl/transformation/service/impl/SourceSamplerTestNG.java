package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.SourceStandardizationFlow;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class SourceSamplerTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SourceSamplerTestNG.class);

    private GeneralSource baseSource = new GeneralSource("Input");
    private GeneralSource source = new GeneralSource("Output");

    private int total = 1000;
    private float sampleFraction = 0.3f;

    @Test(groups = "functional")
    public void testTransformation() {
        prepareData();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("SourceSampler");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(baseSource.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(SourceStandardizationFlow.TRANSFORMER_NAME);
        step0.setTargetSource(source.getSourceName());
        step0.setConfiguration(getStandardizationTransformerConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private String getStandardizationTransformerConfig() {
        StandardizationTransformerConfig config = new StandardizationTransformerConfig();
        config.setSampleFraction(sampleFraction);
        StandardizationTransformerConfig.StandardizationStrategy[] seq = {
                StandardizationTransformerConfig.StandardizationStrategy.SAMPLE };
        config.setSequence(seq);
        return JsonUtils.serialize(config);
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("ID", Integer.class));
        Object[][] data = new Object[total][1];
        for (int i = 0; i < total; i++) {
            data[i][0] = i;
        }
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int cnt = 0;
        Set<Integer> set = new HashSet<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Assert.assertFalse(set.contains((Integer) (record.get("ID"))));
            cnt++;
        }
        Assert.assertTrue(cnt <= total * sampleFraction * 2);
        log.info(String.format("Sampled count %d out of total count %d", cnt, total));
    }
}
