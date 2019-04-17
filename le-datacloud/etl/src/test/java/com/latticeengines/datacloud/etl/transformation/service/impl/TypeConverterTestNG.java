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
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
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
        String[] convertTypeFields = new String[] {
                "Col0", "Col1", "Col2", "Col3", "Col4", "Col5", //
                "Col28", "Col23", "Col8", "Col20", "Col21", "Col22", //
                "Col29", "Col9", "Col18", "Col19", "Col33", //
                "Col32", "Col7", "Col12", "Col13", "Col14", "Col34", "Col35", "Col36", "Col37", "Col38", "Col39", "Col40", //
                "Col31", "Col26", "Col15", "Col16", "Col17", "Col6", "Col10", "Col11" //
        };
        conf.setConvertTypeFields(convertTypeFields);
        TypeConvertStrategy[] convertTypeStrategies = new TypeConvertStrategy[] {
                TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING, TypeConvertStrategy.ANY_TO_STRING, //
                TypeConvertStrategy.ANY_TO_LONG, TypeConvertStrategy.ANY_TO_LONG, TypeConvertStrategy.ANY_TO_LONG, TypeConvertStrategy.ANY_TO_LONG, TypeConvertStrategy.ANY_TO_LONG, TypeConvertStrategy.ANY_TO_LONG, //
                TypeConvertStrategy.ANY_TO_DOUBLE, TypeConvertStrategy.ANY_TO_DOUBLE, TypeConvertStrategy.ANY_TO_DOUBLE, TypeConvertStrategy.ANY_TO_DOUBLE, TypeConvertStrategy.ANY_TO_DOUBLE, //
                TypeConvertStrategy.ANY_TO_BOOLEAN, TypeConvertStrategy.ANY_TO_BOOLEAN,
                TypeConvertStrategy.ANY_TO_BOOLEAN, TypeConvertStrategy.ANY_TO_BOOLEAN,
                TypeConvertStrategy.ANY_TO_BOOLEAN, TypeConvertStrategy.ANY_TO_BOOLEAN,
                TypeConvertStrategy.ANY_TO_BOOLEAN, TypeConvertStrategy.ANY_TO_BOOLEAN,
                TypeConvertStrategy.ANY_TO_BOOLEAN, TypeConvertStrategy.ANY_TO_BOOLEAN,
                TypeConvertStrategy.ANY_TO_BOOLEAN, TypeConvertStrategy.ANY_TO_BOOLEAN, //
                TypeConvertStrategy.ANY_TO_INT, TypeConvertStrategy.ANY_TO_INT, TypeConvertStrategy.ANY_TO_INT, TypeConvertStrategy.ANY_TO_INT, TypeConvertStrategy.ANY_TO_INT, TypeConvertStrategy.ANY_TO_INT, TypeConvertStrategy.ANY_TO_INT, TypeConvertStrategy.ANY_TO_INT //
        };
        conf.setConvertTypeStrategies(convertTypeStrategies);
        return JsonUtils.serialize(conf);
    }

    private void prepareData() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        // AnyToString
        schema.add(Pair.of("Col0", Integer.class));
        schema.add(Pair.of("Col1", String.class));
        schema.add(Pair.of("Col2", Integer.class));
        schema.add(Pair.of("Col3", Long.class));
        schema.add(Pair.of("Col4", Float.class));
        schema.add(Pair.of("Col5", Double.class));
        // AnyToLong
        schema.add(Pair.of("Col28", Long.class));
        schema.add(Pair.of("Col23", String.class));
        schema.add(Pair.of("Col8", String.class));
        schema.add(Pair.of("Col20", Double.class));
        schema.add(Pair.of("Col21", Float.class));
        schema.add(Pair.of("Col22", Integer.class));
        // AnyToDouble
        schema.add(Pair.of("Col29", Double.class));
        schema.add(Pair.of("Col9", Integer.class));
        schema.add(Pair.of("Col18", Long.class));
        schema.add(Pair.of("Col19", String.class));
        schema.add(Pair.of("Col33", Float.class));
        // AnyToBoolean
        schema.add(Pair.of("Col32", Boolean.class));
        schema.add(Pair.of("Col7", Integer.class));
        schema.add(Pair.of("Col12", Long.class));
        schema.add(Pair.of("Col13", String.class));
        schema.add(Pair.of("Col14", Boolean.class));
        schema.add(Pair.of("Col34", String.class));
        schema.add(Pair.of("Col35", String.class));
        schema.add(Pair.of("Col36", String.class));
        schema.add(Pair.of("Col37", String.class));
        schema.add(Pair.of("Col38", String.class));
        schema.add(Pair.of("Col39", String.class));
        schema.add(Pair.of("Col40", String.class));
        // AnyToInt
        schema.add(Pair.of("Col31", Integer.class));
        schema.add(Pair.of("Col26", String.class));
        schema.add(Pair.of("Col15", Float.class));
        schema.add(Pair.of("Col16", Boolean.class));
        schema.add(Pair.of("Col17", Long.class));
        schema.add(Pair.of("Col6", String.class));
        schema.add(Pair.of("Col10", Double.class));
        schema.add(Pair.of("Col11", Long.class));

        Object[][] data = new Object[][] {
            {
                // AnyToString
                null, "1", 1, 1L, 1F, 1D,
                // AnyToLong
                6L, null, "2", 7D, 4F, 3,
                // AnyToDouble
                8D, null, 5L, "89", 6F,
                // AnyToBoolean
                null, 0, 1L, "Y", true, "YES", "TRUE", "1", "N", "NO", "FALSE", "0",
                // AnyToInt
                12, null, 2F, true, 100L, "2", 6D, 3L
            }
        };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int cnt = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            // AnyToString
            Assert.assertTrue(isObjEquals(record.get("Col0"), null));
            Assert.assertTrue(isObjEquals(record.get("Col1"), "1"));
            Assert.assertTrue(isObjEquals(record.get("Col2"), "1"));
            Assert.assertTrue(isObjEquals(record.get("Col3"), "1"));
            Assert.assertTrue(isObjEquals(record.get("Col4"), "1.0"));
            Assert.assertTrue(isObjEquals(record.get("Col5"), "1.0"));
            // AnyToLong
            Assert.assertTrue(isObjEquals(record.get("Col28"), 6L));
            Assert.assertTrue(isObjEquals(record.get("Col23"), null));
            Assert.assertTrue(isObjEquals(record.get("Col8"), 2L));
            Assert.assertTrue(isObjEquals(record.get("Col20"), 7L));
            Assert.assertTrue(isObjEquals(record.get("Col21"), 4L));
            Assert.assertTrue(isObjEquals(record.get("Col22"), 3L));
            // AnyToDouble
            Assert.assertTrue(isObjEquals(record.get("Col29"), 8D));
            Assert.assertTrue(isObjEquals(record.get("Col9"), null));
            Assert.assertTrue(isObjEquals(record.get("Col18"), 5D));
            Assert.assertTrue(isObjEquals(record.get("Col19"), 89D));
            Assert.assertTrue(isObjEquals(record.get("Col33"), 6D));
            // AnyToBoolean
            Assert.assertTrue(isObjEquals(record.get("Col32"), null));
            Assert.assertTrue(isObjEquals(record.get("Col7"), new Boolean(false)));
            Assert.assertTrue(isObjEquals(record.get("Col12"), new Boolean(true)));
            Assert.assertTrue(isObjEquals(record.get("Col13"), new Boolean(true)));
            Assert.assertTrue(isObjEquals(record.get("Col14"), new Boolean(true)));
            Assert.assertTrue(isObjEquals(record.get("Col34"), new Boolean(true)));
            Assert.assertTrue(isObjEquals(record.get("Col35"), new Boolean(true)));
            Assert.assertTrue(isObjEquals(record.get("Col36"), new Boolean(true)));
            Assert.assertTrue(isObjEquals(record.get("Col37"), new Boolean(false)));
            Assert.assertTrue(isObjEquals(record.get("Col38"), new Boolean(false)));
            Assert.assertTrue(isObjEquals(record.get("Col39"), new Boolean(false)));
            Assert.assertTrue(isObjEquals(record.get("Col40"), new Boolean(false)));
            // AnyToInt
            Assert.assertTrue(isObjEquals(record.get("Col31"), 12));
            Assert.assertTrue(isObjEquals(record.get("Col26"), null));
            Assert.assertTrue(isObjEquals(record.get("Col15"), 2));
            Assert.assertTrue(isObjEquals(record.get("Col16"), 1));
            Assert.assertTrue(isObjEquals(record.get("Col17"), 100));
            Assert.assertTrue(isObjEquals(record.get("Col6"), 2));
            Assert.assertTrue(isObjEquals(record.get("Col10"), 6));
            Assert.assertTrue(isObjEquals(record.get("Col11"), 3));
            cnt++;
        }
        Assert.assertEquals(cnt, 1);
    }
}
