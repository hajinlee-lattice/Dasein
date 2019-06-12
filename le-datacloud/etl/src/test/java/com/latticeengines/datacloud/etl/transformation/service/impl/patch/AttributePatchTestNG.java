package com.latticeengines.datacloud.etl.transformation.service.impl.patch;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.dataflow.transformation.patch.AttributePatch;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceIngestion;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AttributePatchTestNG extends PipelineTransformationTestNGBase {

    // sampled account master avro file and the expected row count
    private static final String REAL_AM_TEST_FILE = "AccountMaster206";
    private static final int REAL_AM_TEST_ROWS = 245;

    private static final String AM_DOMAIN = MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain);
    private static final String AM_DUNS = MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS);
    // supported types => test value in AM, will generate one AM column per type, each row either has the test value or
    // null.
    // NOTE linked hash map is used to guarantee the order
    private static final Map<Class<?>, Object> SUPPORTED_TYPES = new LinkedHashMap<>();
    private static final List<Pair<String, Class<?>>> AM_SCHEMA = new ArrayList<>();
    private static final Map<String, String> AM_SCHEMA_PROPERTIES = new HashMap<>();
    // type => position when iterating SUPPORTED_TYPES
    private static final Map<Class<?>, Integer> TYPE_POSITION_MAP = new HashMap<>();
    // column name => position in AM
    private static final Map<String, Integer> COLUMN_POSITION_MAP = new HashMap<>();

    /*
     * Sources for fake account master data
     */
    private IngestionSource attrPatchBookForFakeAM = new IngestionSource("AttributePatchBookForFakeAM");
    private GeneralSource fakeAM = new GeneralSource("FakeAccountMaster");
    private GeneralSource fakePatchedAM = new GeneralSource("FakeAccountMasterPatched");
    private GeneralSource fakePatchedAMTez = new GeneralSource("FakeAccountMasterPatchedTez");
    /*
     * Sources for real account master data (sampled)
     */
    private IngestionSource attrPatchBookForRealAM = new IngestionSource("AttributePatchBookForRealAM");
    private GeneralSource realAM = new GeneralSource("RealAccountMaster");
    private GeneralSource realPatchedAM = new GeneralSource("RealAccountMasterPatched");

    static {
        // initialize supported types and schema

        SUPPORTED_TYPES.put(Boolean.class, false);
        SUPPORTED_TYPES.put(Integer.class, 100);
        SUPPORTED_TYPES.put(Long.class, 9999L);
        SUPPORTED_TYPES.put(Double.class, Math.PI);
        SUPPORTED_TYPES.put(String.class, "helloworld");

        AM_SCHEMA.add(Pair.of(AM_DOMAIN, String.class));
        AM_SCHEMA.add(Pair.of(AM_DUNS, String.class));
        Iterator<Class<?>> it = SUPPORTED_TYPES.keySet().iterator();
        // start with 2 to offset domain & DUNS columns
        for (int i = 2; it.hasNext(); i++) {
            Class<?> clz = it.next();
            TYPE_POSITION_MAP.put(clz, i);
            AM_SCHEMA.add(Pair.of(clz.getSimpleName(), clz));
            COLUMN_POSITION_MAP.put(clz.getSimpleName(), i);
        }

        // add schema properties to make sure they are inherited
        AM_SCHEMA_PROPERTIES.put("DataCloudVersion", "2.0.15");
        AM_SCHEMA_PROPERTIES.put("TestProp1", "TestVal1");
        AM_SCHEMA_PROPERTIES.put("TestProp2", "TestVal2");
    }

    @Override
    protected String getTargetSourceName() {
        return fakePatchedAMTez.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("AttributePatch");
        configuration.setVersion(targetVersion);

        // Fake AM + Flink
        TransformationStepConfig step0 = new TransformationStepConfig();
        step0.setBaseSources(Arrays
                .asList(fakeAM.getSourceName(), attrPatchBookForFakeAM.getSourceName()));
        step0.setBaseIngestions(Collections.singletonMap(attrPatchBookForFakeAM.getSourceName(),
                new SourceIngestion(attrPatchBookForFakeAM.getIngestionName())));
        step0.setTransformer(AttributePatch.TRANSFORMER_NAME);
        step0.setTargetSource(fakePatchedAM.getSourceName());
        step0.setConfiguration(getConfigStr());

        // Fake AM + TEZ (test TEZ's compatibility)
        TransformationStepConfig step1 = new TransformationStepConfig();
        step1.setBaseSources(Arrays
                .asList(fakeAM.getSourceName(), attrPatchBookForFakeAM.getSourceName()));
        step1.setBaseIngestions(Collections.singletonMap(attrPatchBookForFakeAM.getSourceName(),
                new SourceIngestion(attrPatchBookForFakeAM.getIngestionName())));
        step1.setTransformer(AttributePatch.TRANSFORMER_NAME);
        step1.setTargetSource(fakePatchedAMTez.getSourceName());
        step1.setConfiguration(setDataFlowEngine(getConfigStr(), "TEZ"));

        // Real AM + TEZ (large # of columns)
        TransformationStepConfig step2 = new TransformationStepConfig();
        step2.setBaseSources(Arrays
                .asList(realAM.getSourceName(), attrPatchBookForRealAM.getSourceName()));
        step2.setBaseIngestions(Collections.singletonMap(attrPatchBookForRealAM.getSourceName(),
                new SourceIngestion(attrPatchBookForRealAM.getIngestionName())));
        step2.setTransformer(AttributePatch.TRANSFORMER_NAME);
        step2.setTargetSource(realPatchedAM.getSourceName());
        step2.setConfiguration(setDataFlowEngine(getConfigStr(), "TEZ"));

        // -----------
        List<TransformationStepConfig> steps = Arrays.asList(step0, step1, step2);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    /*
     * Test the correctness of AttributePatch transformer's functionality. There are three different settings:
     * 1. Fake AM running in memory (Flink) to test the correctness of attribute patch
     * 2. Fake AM running in yarn (TEZ) to test the correctness under TEZ engine
     * 3. Real AM running in yarn (TEZ) to test the correctness when the number of column is large
     */
    @Test(groups = "pipeline1")
    public void testTransformation() {
        // upload data for fake & real AM
        prepareAttributePatchBook(attrPatchBookDataForFakeAM, attrPatchBookForFakeAM);
        uploadBaseSourceData(fakeAM.getSourceName(), baseSourceVersion, AM_SCHEMA, fakeAMData);
        generateSchemaFile(fakeAM, baseSourceVersion, AM_SCHEMA_PROPERTIES);
        prepareAttributePatchBook(attrPatchBookDataForRealAM, attrPatchBookForRealAM);
        uploadBaseSourceFile(realAM.getSourceName(), REAL_AM_TEST_FILE, baseSourceVersion);
        generateSchemaFile(realAM, baseSourceVersion, AM_SCHEMA_PROPERTIES);
        // perform transformation
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        // check if the workflow job succeeded
        finish(progress);
        // check if everything is patched correctly
        confirmIntermediateSource(fakePatchedAM, targetVersion);
        confirmIntermediateSource(fakePatchedAMTez, targetVersion);
        confirmIntermediateSource(realPatchedAM, targetVersion);
        cleanupProgressTables();
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        // verify schema properties are inherited
        verifySchemaProperties(new GeneralSource(source), version);
        if (fakePatchedAM.getSourceName().equals(source) || fakePatchedAMTez.getSourceName().equals(source)) {
            verifyFakeAMTestData(records);
        } else if (realPatchedAM.getSourceName().equals(source)) {
            verifyRealAMTestData(records);
        } else {
            throw new UnsupportedOperationException("Unknown source name: " + source);
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        // do nothing since we are using verifyIntermediateResult to check the results
    }

    private void prepareAttributePatchBook(Object[][] patchBooks, Source baseSource) {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(DataCloudConstants.ATTR_PATCH_DOMAIN, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_PATCH_DUNS, String.class));
        schema.add(Pair.of(DataCloudConstants.ATTR_PATCH_ITEMS, String.class));
        uploadBaseSourceData(baseSource, baseSourceVersion, schema, patchBooks);
    }

    /*
     * Check whether the real AM data is patched correctly
     */
    private void verifyRealAMTestData(Iterator<GenericRecord> records) {
        Schema amSchema = getRealAMSchema();
        List<Schema.Field> amFields = amSchema.getFields();
        Map<String, Map<String, Object>> expectedPatchedAttributeMap = getPatchedAttributeMapForRealAM();
        int total = 0;
        for (; records.hasNext(); total++) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record);
            Schema schema = record.getSchema();
            List<Schema.Field> fields = schema.getFields();
            Assert.assertNotNull(fields);
            Assert.assertEquals(fields.size(), amFields.size());
            IntStream.range(0, amFields.size()).forEach(idx -> {
                Schema.Field field = fields.get(idx);
                Schema.Field expectedField = amFields.get(idx);
                Assert.assertEquals(field.name(), expectedField.name());
                Assert.assertEquals(field.pos(), expectedField.pos());
                // only check one layer
                Assert.assertEquals(field.schema().getType(), expectedField.schema().getType());
            });

            String domain = toString(record.get(AM_DOMAIN));
            String duns = toString(record.get(AM_DUNS));
            String key = getKey(domain, duns);
            if (expectedPatchedAttributeMap.containsKey(key)) {
                expectedPatchedAttributeMap.get(key).forEach((attr, expectedValue) -> {
                    Object attrValue = record.get(attr);
                    Assert.assertTrue(isObjEquals(attrValue, expectedValue));
                });
                expectedPatchedAttributeMap.remove(key);
            }
        }
        Assert.assertEquals(total, REAL_AM_TEST_ROWS);
        // all rows patched
        Assert.assertTrue(expectedPatchedAttributeMap.isEmpty());
    }

    private Schema getRealAMSchema() {
        try {
            InputStream is = ClassLoader.getSystemResourceAsStream(String.format("sources/%s.avro", REAL_AM_TEST_FILE));
            return AvroUtils.readSchemaFromInputStream(is);
        } catch (IOException e) {
            // rethrow and fail the test
            throw new RuntimeException(e);
        }
    }

    /*
     * Generate Avro schema file for the input source and add all input properties to the schema file
     */
    private void generateSchemaFile(
            @NotNull Source source, @NotNull String version, @NotNull Map<String, String> properties) {
        try {
            String avroDir = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
            List<String> srcFiles = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
            Schema srcSchema = AvroUtils.getSchema(yarnConfiguration, new Path(srcFiles.get(0)));
            properties.forEach(srcSchema::addProp);

            String avscPath = hdfsPathBuilder.constructSchemaFile(source.getSourceName(), version)
                    .toString();
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, srcSchema.toString());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create schema file for AccountMaster", e);
        }
    }

    /*
     * Check whether the fake AM data is patched correctly
     */
    private void verifyFakeAMTestData(Iterator<GenericRecord> records) {
        Map<String, Object[]> expectedResults = getExpectedResults();
        Assert.assertNotNull(records);
        int total = 0;
        for (; records.hasNext(); total++) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record);
            String domain = toString(record.get(AM_DOMAIN));
            String duns = toString(record.get(AM_DUNS));
            // cannot be both null
            Assert.assertTrue(domain != null || duns != null);
            String key = getKey(domain, duns);
            Object[] expectedRow = expectedResults.get(key);
            Assert.assertNotNull(expectedRow);
            // all attributes have the correct value
            for (Pair<String, Class<?>> col : AM_SCHEMA) {
                String colName = col.getKey();
                if (AM_DOMAIN.equals(colName) || AM_DUNS.equals(colName)) {
                    // no need to verify, if we can retrieve expected row domain + DUNS is a match
                    continue;
                }
                Object colValue = record.get(colName);
                Assert.assertTrue(isObjEquals(colValue, expectedRow[COLUMN_POSITION_MAP.get(colName)]));
            }
            // check the columns
            verifySchema(record.getSchema(), AM_SCHEMA);
        }
        Assert.assertEquals(total, expectedResults.size());
    }

    /*
     * Check whether the input schema contains the same fields (same fieldName and type) in the same order
     */
    private void verifySchema(Schema schema, List<Pair<String, Class<?>>> expectedSchema) {
        Assert.assertNotNull(schema);
        Assert.assertNotNull(schema.getFields());
        Map<String, Schema.Field> fieldMap = schema
                .getFields()
                .stream()
                .collect(Collectors.toMap(Schema.Field::name, field -> field));
        Assert.assertEquals(fieldMap.size(), expectedSchema.size());
        expectedSchema.forEach(pair -> {
            // check field name and type
            Schema.Field field = fieldMap.get(pair.getKey());
            Assert.assertNotNull(field);
            Schema.Type type = field.schema().getType();
            Schema.Type expectedType = AvroUtils.getAvroType(pair.getValue());
            Assert.assertNotNull(type);
            if (type == Schema.Type.UNION) {
                Optional<Schema> matchedNestedType = field.schema()
                        .getTypes()
                        .stream()
                        .filter(nestedSchema -> nestedSchema != null && expectedType.equals(nestedSchema.getType()))
                        .findAny();
                // found the expected type in union
                Assert.assertTrue(matchedNestedType.isPresent());
            } else {
                Assert.assertEquals(type, expectedType);
            }
        });
    }

    /*
     * Verify that all AM schema properties are inherited in the specified source schema file
     */
    private void verifySchemaProperties(Source source, String version) {
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(source, version);
        Assert.assertNotNull(schema);
        Assert.assertNotNull(schema.getJsonProps());
        AM_SCHEMA_PROPERTIES.forEach((key, val) -> {
            // property is inherited
            Assert.assertEquals(schema.getProp(key), val);
        });
    }

    private String getConfigStr() {
        TransformerConfig config = new TransformerConfig();
        // need to retain json properties from AM
        config.setShouldInheritSchemaProp(true);
        return JsonUtils.serialize(config);
    }

    /*
     * Create a attribute patch book row with patchItems specified by the given types/values
     */
    private Object[] newAttrPatchBookRow(
            String domain, String duns, @NotNull Class<?>[] classes, @NotNull Object[] values) {
        Preconditions.checkNotNull(classes);
        Preconditions.checkNotNull(values);
        Preconditions.checkArgument(classes.length == values.length && classes.length > 0);
        Map<String, Object> patchItems = IntStream
                .range(0, classes.length)
                .mapToObj(idx -> Pair.of(classes[idx].getSimpleName(), values[idx]))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        return new Object[] { domain, duns, JsonUtils.serialize(patchItems) };
    }

    /*
     * key = [ Domain + DUNS ], value = expected AM row after patching
     */
    private Map<String, Object[]> getExpectedResults() {
        return Arrays
                .stream(expectedPatchedFakeAMData)
                .map(row -> Pair.of(getKey((String) row[0], (String) row[1]), row))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private String getKey(String domain, String duns) {
        return String.format("Domain=%s,DUNS=%s", domain, duns);
    }

    private String toString(Object obj) {
        return obj == null ? null : obj.toString();
    }

    /*
     * retrieve the default test value of a given type
     */
    private <T> T testVal(Class<T> clz) {
        return clz.cast(SUPPORTED_TYPES.get(clz));
    }

    /*
     * helper to create an account master row with given attribute name. values will be set to the default test value
     */
    private Object[] newAMRow(String domain, String duns, Class<?>... classes) {
        return newAMRow(domain, duns, classes, Arrays.stream(classes).map(SUPPORTED_TYPES::get).toArray());
    }

    /*
     * helper to create an account master row with given attribute name and value
     */
    private Object[] newAMRow(String domain, String duns, @NotNull Class<?>[] classes, @NotNull Object[] values) {
        // domain + DUNS + one column per supported type
        Preconditions.checkNotNull(classes);
        Preconditions.checkNotNull(values);
        Preconditions.checkArgument(classes.length == values.length);
        // domain + DUNS + one column per supported type
        Object[] row = new Object[SUPPORTED_TYPES.size() + 2];
        row[0] = domain;
        row[1] = duns;
        IntStream.range(0, classes.length).forEach(idx -> row[TYPE_POSITION_MAP.get(classes[idx])] = values[idx]);
        return row;
    }

    /*
     * { key(Domain+DUNS) => { attribute name => patched value } }
     *
     * NOTE only check the patched columns for real AM as the main purpose is to make sure transformer works with
     * the real data
     */
    private Map<String, Map<String, Object>> getPatchedAttributeMapForRealAM() {
        Map<String, Map<String, Object>> map = new HashMap<>();
        String key1 = getKey("cowinadv.com", "663428316");
        map.put(key1, new HashMap<>());
        map.get(key1).put("Bmbr30_AdTech_Total", 55);
        map.get(key1).put("SALES_VOLUME_US_DOLLARS", 12345678L);
        map.get(key1).put("AlexaRank", 99);
        map.get(key1).put("AlexaDescription", "test");

        String key2 = getKey(null, "902984087");
        map.put(key2, new HashMap<>());
        map.get(key2).put("SALES_VOLUME_US_DOLLARS", 333L);
        map.get(key2).put("EMPLOYEES_TOTAL", "5678");

        String key3 = getKey("laseresaude.com.br", null);
        map.put(key3, new HashMap<>());
        map.get(key3).put("LE_EMPLOYEE_RANGE", "50-100");

        String key4 = getKey("perfectinnovations.site40.net", null);
        map.put(key4, new HashMap<>());
        map.get(key4).put("SALES_VOLUME_US_DOLLARS", 555555L);

        String key5 = getKey(null, "722669306");
        map.put(key5, new HashMap<>());
        map.get(key5).put("SALES_VOLUME_US_DOLLARS", 555555L);
        return map;
    }

    /*
     * Test data to test with real AM data (large dataset) to make sure transformer finishes successfully when the
     * number of column is large.
     */
    private Object[][] attrPatchBookDataForRealAM = new Object[][] {
            // NOTE use real data so that things are actually patched
            // #1: will patch Domain=cowinadv.com, DUNS=663428316
            { "cowinadv.com", null, "{\"Bmbr30_AdTech_Total\":\"55\", \"SALES_VOLUME_US_DOLLARS\":12345678}" },
            { "cowinadv.com", "663428316", "{\"AlexaRank\":99}" },
            { null, "663428316", "{\"AlexaDescription\":\"test\"}" },
            // #2: will patch Domain=NULL, DUNS=902984087
            { null, "902984087", "{\"SALES_VOLUME_US_DOLLARS\":333, \"EMPLOYEES_TOTAL\":5678}" },
            // #3: will patch Domain=laseresaude.com.br, DUNS=null
            { "laseresaude.com.br", null, "{\"LE_EMPLOYEE_RANGE\":\"50-100\"}" },
            // #4: will patch Domain=perfectinnovations.site40.net, DUNS=null
            { "perfectinnovations.site40.net", null, "{\"SALES_VOLUME_US_DOLLARS\":555555}" },
            // #5: will patch Domain=NULL, DUNS=722669306
            { null, "722669306", "{\"SALES_VOLUME_US_DOLLARS\":555555}" },
    };

    /*
     * Test data for testing the correctness of Attribute Patch Transformer's functionality
     */

    private Object[][] attrPatchBookDataForFakeAM = new Object[][] {
            // NOTE cannot have duplicate fields when patching the same case
            // Patch Case #1, #7
            newAttrPatchBookRow(
                    "google.com", null,
                    new Class<?>[]{ Long.class, String.class },
                    new Object[] { 123L, "prometheus.io" }),
            // Patch Case #2, #5, #6
            newAttrPatchBookRow(
                    null, "111111111",
                    new Class<?>[]{ String.class },
                    new Object[] { "kubernetes" }),
            // Patch Case #3
            newAttrPatchBookRow(
                    "reddit.com", "222222222",
                    new Class<?>[]{ Boolean.class, Integer.class, Long.class, Double.class, String.class },
                    // (a) string -> boolean, int, double
                    // (b) boolean -> string
                    new Object[] { "FALSE", "123.45", 543, "1.4142", true }),
            // Patch Case #4
            newAttrPatchBookRow(
                    "facebook.com", null,
                    new Class<?>[]{ Boolean.class, Long.class, Double.class, String.class },
                    // (a) number string -> boolean
                    // (b) double string -> int
                    // (c) scientific notation string -> double
                    // (d) int -> string
                    new Object[] { "1", "0.87", "1e4", -100 }),
            newAttrPatchBookRow(
                    "facebook.com", "333333333",
                    new Class<?>[]{ Integer.class },
                    new Object[] { 9527 }),
            // Patch Case #5
            newAttrPatchBookRow(
                    "netflix.com", "111111111",
                    new Class<?>[]{ Integer.class, Double.class },
                    // int string -> int
                    new Object[] { "3", 123.45 }),
            // Patch Case #6
            newAttrPatchBookRow(
                    "istio.io", null,
                    new Class<?>[]{ Boolean.class, Long.class, Double.class },
                    // (a) enum boolean string -> boolean
                    // (b) int -> long, double
                    new Object[] { "N", 999, 5566 }),
            // Patch Case #7
            newAttrPatchBookRow(
                    "google.com", "444444444",
                    new Class<?>[]{ Boolean.class },
                    // enum boolean string -> boolean
                    new Object[] { "YES" }),
            newAttrPatchBookRow(
                    null, "444444444",
                    new Class<?>[]{ Integer.class, Double.class },
                    new Object[] { "3", 123.45 }),
    };

    private Object[][] fakeAMData = new Object[][] {
            // Case #1: patched by Domain only (with & without DUNS)
            newAMRow("google.com", null, Boolean.class, Long.class),
            newAMRow("google.com", "000000000", Boolean.class, Integer.class, String.class),
            // Case #2: patched by DUNS only (with & without Domain)
            newAMRow(null, "111111111", Integer.class, Double.class),
            newAMRow("twitter.com", "111111111", Boolean.class, Long.class, Double.class, String.class),
            // Case #3: patched by Domain + DUNS
            newAMRow("reddit.com", "222222222", Integer.class),
            // Case #4: patched by Domain only & Domain + DUNS
            newAMRow("facebook.com", "333333333", Integer.class, String.class),
            // Case #5: patched by DUNS only & Domain + DUNS
            newAMRow("netflix.com", "111111111", Integer.class, Long.class, Double.class),
            // Case #6: patched by Domain only & DUNS only
            newAMRow("istio.io", "111111111", Boolean.class, String.class),
            // Case #7: patched by Domain, DUNS, Domain + DUNS
            newAMRow("google.com", "444444444",
                    Boolean.class, Integer.class, Long.class, Double.class, String.class),
            // Case #8: did not get patched by anything
            newAMRow("fakedomain.com", "999999999", String.class),
    };

    private Object[][] expectedPatchedFakeAMData = new Object[][] {
            // Case #1: patched by Domain only (with & without DUNS)
            newAMRow(
                    "google.com", null,
                    new Class<?>[] { Boolean.class, Long.class, String.class },
                    new Object[] { testVal(Boolean.class), 123L, "prometheus.io" }),
            newAMRow(
                    "google.com", "000000000",
                    new Class<?>[] { Boolean.class, Integer.class, Long.class, String.class },
                    new Object[] { testVal(Boolean.class), testVal(Integer.class), 123L, "prometheus.io" }),
            // Case #2: patched by DUNS only (with & without Domain)
            newAMRow(
                    null, "111111111",
                    new Class<?>[] { Integer.class, Double.class, String.class },
                    new Object[] { testVal(Integer.class), testVal(Double.class), "kubernetes" }),
            newAMRow(
                    "twitter.com", "111111111",
                    new Class<?>[] { Boolean.class, Long.class, Double.class, String.class },
                    new Object[] { testVal(Boolean.class), testVal(Long.class), testVal(Double.class), "kubernetes" }),
            // Case #3: patched by Domain + DUNS
            newAMRow(
                    "reddit.com", "222222222",
                    new Class<?>[]{ Boolean.class, Integer.class, Long.class, Double.class, String.class },
                    new Object[] { false, 123, 543L, 1.4142, "true" }),
            // Case #4: patched by Domain only & Domain + DUNS
            newAMRow(
                    "facebook.com", "333333333",
                    new Class<?>[]{ Boolean.class, Integer.class, Long.class, Double.class, String.class },
                    new Object[] { true, 9527, 0L, 10000.0, "-100" }),
            // Case #5: patched by DUNS only & Domain + DUNS
            newAMRow(
                    "netflix.com", "111111111",
                    new Class<?>[]{ Integer.class, Long.class, Double.class, String.class },
                    new Object[] { 3, testVal(Long.class), 123.45, "kubernetes" }),
            // Case #6: patched by Domain only & DUNS only
            newAMRow(
                    "istio.io", "111111111",
                    new Class<?>[]{ Boolean.class, Long.class, Double.class, String.class },
                    new Object[] { false, 999L, 5566.0, "kubernetes" }),
            // Case #7: patched by Domain, DUNS, Domain + DUNS
            newAMRow(
                    "google.com", "444444444",
                    new Class<?>[]{ Boolean.class, Integer.class, Long.class, Double.class, String.class },
                    new Object[] { true, 3, 123L, 123.45, "prometheus.io" }),
            // Case #8: did not get patched by anything
            newAMRow(
                    "fakedomain.com", "999999999",
                    new Class<?>[] { String.class },
                    new Object[] { testVal(String.class) }),
    };
}
