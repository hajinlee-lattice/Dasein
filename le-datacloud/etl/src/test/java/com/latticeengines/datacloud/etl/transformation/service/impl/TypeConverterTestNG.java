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

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class TypeConverterTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TypeConverterTestNG.class);

    private GeneralSource baseSource = new GeneralSource("TestInput");
    private GeneralSource source = new GeneralSource("TestOutput");

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
        configuration.setName("TypeConverter");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(baseSource.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step0.setTargetSource(source.getSourceName());
        step0.setConfiguration(getTypeConvertConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);

        // -----------
        configuration.setSteps(steps);
        configuration.setKeepTemp(true);
        return configuration;
    }

    private String getTypeConvertConfig() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        StandardizationStrategy[] sequence = new StandardizationStrategy[] { StandardizationStrategy.CONVERT_TYPE };
        conf.setSequence(sequence);
        String[] convertTypeFields = new String[] { "Col0", "Col1", "Col2", "Col3", "Col4", "Col5" };
        conf.setConvertTypeFields(convertTypeFields);
        TypeConvertStrategy[] convertTypeStrategies = new TypeConvertStrategy[] { TypeConvertStrategy.ANY_TO_STRING,
                TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING,
                TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING };
        conf.setConvertTypeStrategies(convertTypeStrategies);
        return JsonUtils.serialize(conf);
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("Col0", Integer.class));
        schema.add(Pair.of("Col1", String.class));
        schema.add(Pair.of("Col2", Integer.class));
        schema.add(Pair.of("Col3", Long.class));
        schema.add(Pair.of("Col4", Float.class));
        schema.add(Pair.of("Col5", Double.class));

        Object[][] data = new Object[][] {
                { null, "1", 1, 1L, 1F, 1D }
        };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int cnt = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertTrue(isObjEquals(record.get("Col0"), null));
            Assert.assertTrue(isObjEquals(record.get("Col1"), "1"));
            Assert.assertTrue(isObjEquals(record.get("Col2"), "1"));
            Assert.assertTrue(isObjEquals(record.get("Col3"), "1"));
            Assert.assertTrue(isObjEquals(record.get("Col4"), "1.0"));
            Assert.assertTrue(isObjEquals(record.get("Col5"), "1.0"));
            cnt++;
        }
        Assert.assertEquals(cnt, 1);
    }
}
