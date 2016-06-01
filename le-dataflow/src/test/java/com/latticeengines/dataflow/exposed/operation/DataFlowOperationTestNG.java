package com.latticeengines.dataflow.exposed.operation;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.query.ReferenceInterpretation;
import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotType;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;

public class DataFlowOperationTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional", enabled = true)
    public void testSort() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                List<SingleReferenceLookup> lookups = new ArrayList<>();
                lookups.add(new SingleReferenceLookup("Email", ReferenceInterpretation.COLUMN));
                Sort sort = new Sort(lookups);
                return lead.sort(sort);
            }
        });

        List<GenericRecord> output = readOutput();
        String lastEmail = null;
        for (GenericRecord record : output) {
            String email = record.get("Email").toString();
            if (lastEmail != null) {
                Assert.assertTrue(email.compareTo(lastEmail) >= 0);
            }
            lastEmail = email;
        }
    }

    @Test(groups = "functional", enabled = true)
    public void testStopListAllFilter() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                Node lead2 = addSource("Lead").renamePipe("Lead2");
                return lead.stopList(lead2, "Id", "Id");
            }
        });

        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), 0);
    }

    @Test(groups = "functional", enabled = true)
    public void testStopListAllPass() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                Node contact = addSource("Contact");
                return lead.stopList(contact, "Id", "Id");
            }
        });

        List<GenericRecord> input = readInput("Lead");
        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), input.size());
    }

    @Test(groups = "functional", enabled = true)
    public void testStopListAllPassBothLeftAndRightShareColumn() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                Node contact = addSource("Contact");
                return lead.stopList(contact, "Id", "Email");
            }
        });

        List<GenericRecord> input = readInput("Lead");
        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), input.size());
    }

    @Test(groups = "functional", enabled = true)
    public void testGroupByAndLimit() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                return lead.groupByAndLimit(new FieldList("Email"), 1);
            }
        });

        List<GenericRecord> input = readInput("Lead");
        List<GenericRecord> output = readOutput();
        Assert.assertNotEquals(input.size(), output.size());
        final Map<Object, Integer> histogram = histogram(output, "Email");
        Assert.assertTrue(Iterables.all(histogram.keySet(), new Predicate<Object>() {

            @Override
            public boolean apply(@Nullable Object input) {
                return histogram.get(input) == 1;
            }
        }));
    }

    @Test(groups = "functional", enabled = true)
    public void testRename() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                return lead.rename(new FieldList("Email"), new FieldList("Foo"));
            }
        });

        Schema schema = getOutputSchema();
        Assert.assertNotNull(schema.getField("Foo"));
        Assert.assertNull(schema.getField("Email"));
    }

    @Test(groups = "functional", enabled = true)
    public void testAddRowId() throws IOException {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareAddRowId(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                return feature.addRowID(new FieldMetadata("RowID", Long.class));
            }
        });

        List<GenericRecord> output = readOutput();
        Set<Long> ids = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L));
        for (GenericRecord record : output) {
            System.out.println(record);
            ids.remove(record.get("RowID"));
        }

        Assert.assertTrue(ids.isEmpty());

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional", enabled = true)
    public void testAddRowIdParallel() throws InterruptedException, IOException, ExecutionException {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";
        Integer numThreads = 2;

        prepareAddRowId(avroDir, fileName);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<String>> exceptions = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            exceptions.add(executor.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    String targetPath = "/tmp/DataFlowOperationTestOutput_" + UUID.randomUUID().toString();
                    try {
                        for (int j = 0; j < 2; j++) {
                            try {
                                HdfsUtils.rmdir(configuration, targetPath);
                            } catch (Exception e) {
                                return "Failed to clean up target path: " + targetPath;
                            }
                            execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
                                @Override
                                public Node construct(DataFlowParameters parameters) {
                                    Node feature = addSource("Feature");
                                    return feature.addRowID(new FieldMetadata("RowID", Long.class));
                                }
                            }, targetPath);
                            List<GenericRecord> output = readOutput(targetPath);
                            Set<Long> ids = new HashSet<>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L));
                            for (GenericRecord record : output) {
                                ids.remove(record.get("RowID"));
                            }
                            Assert.assertTrue(ids.isEmpty(), "Remaining IDs: " + new ArrayList<>(ids));
                        }
                        return "";
                    } catch (Exception e) {
                        return e.getMessage() + "\n" + ExceptionUtils.getFullStackTrace(e);
                    } finally {
                        try {
                            HdfsUtils.rmdir(configuration, targetPath);
                        } catch (Exception e) {
                            return "Failed to clean up target path: " + targetPath;
                        }
                    }
                }
            }));
        }

        for (Future<String> future: exceptions) {
            String exception = future.get();
            Assert.assertTrue(StringUtils.isEmpty(exception), "Error: " + exception);
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    private void prepareAddRowId(String avroDir, String fileName) {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 3, 124L }, { "dom1.com", "k1_low", 3, 124L }, { "dom1.com", "k1_high", 5, 124L },
                { "dom2.com", "f2", 4, 101L }, { "dom2.com", "f3", 2, 102L }, { "dom2.com", "k1_low", 3, 124L }, };

        uploadAvro(data, avroDir, fileName);
    }

    @Test(groups = "functional", enabled = true)
    public void testSimplePivot() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareSimplePivotData(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                Set<String> features = new HashSet<>(Arrays.asList("f1", "f2", "f3", "f4"));
                PivotStrategyImpl mapper = PivotStrategyImpl.any("Feature", "Value", features, Integer.class, 0);
                return feature.pivot(new String[] { "Domain" }, mapper);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
            if ("dom1.com".equals(record.get("Domain").toString())) {
                Assert.assertEquals(record.get("f1"), 1);
                Assert.assertEquals(record.get("f2"), 2);
                Assert.assertEquals(record.get("f3"), 3);
                Assert.assertEquals(record.get("f4"), 0);
            } else if ("dom2.com".equals(record.get("Domain").toString())) {
                Assert.assertEquals(record.get("f1"), 0);
                Assert.assertEquals(record.get("f2"), 4);
                Assert.assertEquals(record.get("f3"), 2);
                Assert.assertEquals(record.get("f4"), 0);
            }
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional", enabled = true)
    public void testMaxPivot() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareMaxPivotData(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                Set<String> features = new HashSet<>(Arrays.asList("f1", "f2", "f3", "f4"));
                PivotStrategyImpl mapper = PivotStrategyImpl.max("Feature", "Value", features, Integer.class, null);
                return feature.pivot(new String[] { "Domain" }, mapper);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
            if ("dom1.com".equals(record.get("Domain").toString())) {
                Assert.assertEquals(record.get("f1"), 2);
                Assert.assertEquals(record.get("f2"), 3);
                Assert.assertEquals(record.get("f3"), 4);
                Assert.assertEquals(record.get("f4"), null);
            }
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional", enabled = true)
    public void testCountPivot() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareCountPivotData(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                Set<String> features = new HashSet<>(Arrays.asList("f1", "f2", "f3", "f4"));
                PivotStrategyImpl mapper = PivotStrategyImpl.count("Feature", "Value", features);
                return feature.pivot(new String[] { "Domain" }, mapper);
            }
        });

        List<GenericRecord> output = readOutput();
        Assert.assertFalse(output.isEmpty());
        for (GenericRecord record : output) {
            System.out.println(record);
            if ("dom1.com".equals(record.get("Domain").toString())) {
                Assert.assertEquals(record.get("f1"), 2);
                Assert.assertEquals(record.get("f2"), 2);
                Assert.assertEquals(record.get("f3"), 2);
                Assert.assertEquals(record.get("f4"), 0);
            } else if ("dom2.com".equals(record.get("Domain").toString())) {
                Assert.assertEquals(record.get("f1"), 1);
                Assert.assertEquals(record.get("f2"), 2);
                Assert.assertEquals(record.get("f3"), 2);
                Assert.assertEquals(record.get("f4"), 0);
            }
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional", enabled = true)
    public void testExistsPivot() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareMaxPivotData(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                Set<String> features = new HashSet<>(Arrays.asList("f1", "f2", "f3", "f4"));
                PivotStrategyImpl mapper = PivotStrategyImpl.exists("Feature", features);
                return feature.pivot(new String[] { "Domain" }, mapper);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
            if ("dom1.com".equals(record.get("Domain").toString())) {
                Assert.assertEquals(record.get("f1"), true);
                Assert.assertEquals(record.get("f2"), true);
                Assert.assertEquals(record.get("f3"), true);
                Assert.assertEquals(record.get("f4"), false);
            }
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional", enabled = true)
    public void testMergingPivot() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareMaxPivotData(avroDir, fileName);

        final List<AbstractMap.SimpleImmutableEntry<String, String>> columnMappings = new ArrayList<>();
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f1", "f1"));
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f2", "f2"));
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f3", "f3"));
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f1", "combo1"));
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f2", "combo1"));
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f3", "combo1"));
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f2", "combo2"));
        columnMappings.add(new AbstractMap.SimpleImmutableEntry<>("f3", "combo2"));

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                Set<String> features = new HashSet<>(Arrays.asList("f1", "f2", "f3", "f4"));
                PivotStrategyImpl mapper = PivotStrategyImpl.withColumnMap("Feature", "Value", features, columnMappings,
                        Integer.class, PivotType.SUM, 0);
                return feature.pivot(new String[] { "Domain" }, mapper);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
            if ("dom1.com".equals(record.get("Domain").toString())) {
                Assert.assertEquals(record.get("f1"), 3);
                Assert.assertEquals(record.get("f2"), 6);
                Assert.assertEquals(record.get("f3"), 5);
                Assert.assertEquals(record.get("f4"), 0);
                Assert.assertEquals(record.get("combo1"), 14);
                Assert.assertEquals(record.get("combo2"), 11);
            }
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional")
    public void testSwapTimestamp() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";
        prepareSimplePivotData(avroDir, fileName);

        Long before = System.currentTimeMillis();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                return feature.addTimestamp("Timestamp");
            }
        });

        Long after = System.currentTimeMillis();

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            Long timestamp = (Long) record.get("Timestamp");
            Assert.assertNotNull(timestamp);
            Assert.assertTrue(timestamp > before);
            Assert.assertTrue(timestamp < after);
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional")
    public void testAddTimestamp() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";
        prepareSimplePivotData(avroDir, fileName);

        Long before = System.currentTimeMillis();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                return feature.addTimestamp("New_Timestamp");
            }
        });

        Long after = System.currentTimeMillis();

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            Long timestamp = (Long) record.get("New_Timestamp");
            Assert.assertNotNull(timestamp);
            Assert.assertTrue(timestamp > before);
            Assert.assertTrue(timestamp < after);
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional")
    public void testTransformCompanyNameLength() throws Exception {
        final TransformDefinition definition = TransformationPipeline.stdLengthCompanyName;
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead2");
                return lead.addTransformFunction("com.latticeengines.transform.v2_0_25.functions", definition);
            }
        });
        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            Assert.assertNotNull(record.get(definition.output));
        }
    }

    @Test(groups = "functional")
    public void testTransformFundingStage() throws Exception {
        final TransformDefinition definition = TransformationPipeline.stdVisidbDsPdFundingstageOrdered;
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead2");
                return lead.addTransformFunction("com.latticeengines.transform.v2_0_25.functions", definition);
            }
        });
        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), 4826);
    }

    @Test(groups = "functional")
    public void testDepivot() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";
        prepareDepivotData(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource("Feature");
                String[] targetFields = new String[] { "Topic", "Score" };
                String[][] sourceFieldTuples = new String[][] { { "Topic1", "Score1" }, { "Topic2", "Score2" },
                        { "Topic3", "Score3" }, { "Topic4", "Score4" } };
                return node.depivot(targetFields, sourceFieldTuples);
            }
        });

        Map<String, Double> expectedMap = new HashMap<>();
        expectedMap.put("topic1", 1.0);
        expectedMap.put("topic2", 2.0);
        expectedMap.put("topic3", 3.0);
        expectedMap.put("topic4", 4.0);
        expectedMap.put("topic5", 1.1);
        expectedMap.put("topic6", 2.1);
        expectedMap.put("topic7", null);

        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), 8);
        for (GenericRecord record : output) {
            System.out.println(record);
            Assert.assertEquals(record.get("Domain").toString(), "dom1.com");
            if (record.get("Topic") == null) {
                Assert.assertEquals(record.get("Score"), 4.1);
            } else {
                Assert.assertEquals(record.get("Score"), expectedMap.get(record.get("Topic").toString()));
            }
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    private void prepareSimplePivotData(String avroDir, String fileName) {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 3, 124L }, { "dom1.com", "k1_low", 3, 124L }, { "dom1.com", "k1_high", 5, 124L },
                { "dom2.com", "f2", 4, 101L }, { "dom2.com", "f3", 2, 102L }, { "dom2.com", "k1_low", 3, 124L }, };

        uploadAvro(data, avroDir, fileName);
    }

    private void prepareMaxPivotData(String avroDir, String fileName) {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 4, 124L }, { "dom1.com", "f1", 2, 129L }, { "dom1.com", "f3", 1, 122L },
                { "dom1.com", "f2", 3, 121L }, { "dom1.com", "f2", 1, 122L }, };

        uploadAvro(data, avroDir, fileName);
    }

    private void prepareCountPivotData(String avroDir, String fileName) {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 4, 124L }, { "dom1.com", "f1", 2, 129L }, { "dom1.com", "f3", 1, 122L },
                { "dom1.com", "f2", 3, 121L }, { "dom1.com", "f2", 2, 122L }, { "dom2.com", "f1", 1, 123L },
                { "dom2.com", "f2", 2, 125L }, { "dom2.com", "f3", 4, 124L }, { "dom2.com", "f3", 1, 122L },
                { "dom2.com", "f2", 3, 121L }, { "dom2.com", "f2", 2, 122L } };

        uploadAvro(data, avroDir, fileName);
    }

    private void prepareDepivotData(String avroDir, String fileName) {
        Object[][] data = new Object[][] {
                { "dom1.com", "topic1", 1.0, "topic2", 2.0, "topic3", 3.0, "topic4", 4.0, 123L },
                { "dom1.com", "topic5", 1.1, "topic6", 2.1, "topic7", null, null, 4.1, 123L } };

        uploadDepivotAvro(data, avroDir, fileName);
    }

    private void uploadAvro(Object[][] data, String avroDir, String fileName) {
        List<GenericRecord> records = new ArrayList<>();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Domain\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Feature\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Value\",\"type\":[\"int\",\"null\"]},"
                + "{\"name\":\"Timestamp\",\"type\":[\"long\",\"null\"]}" + "]}");
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple : data) {
            builder.set("Domain", tuple[0]);
            builder.set("Feature", tuple[1]);
            builder.set("Value", tuple[2]);
            builder.set("Timestamp", tuple[3]);
            records.add(builder.build());
        }

        try {
            AvroUtils.writeToLocalFile(schema, records, fileName);
            if (HdfsUtils.fileExists(configuration, avroDir + "/" + fileName)) {
                HdfsUtils.rmdir(configuration, avroDir + "/" + fileName);
            }
            HdfsUtils.copyLocalToHdfs(configuration, fileName, avroDir + "/" + fileName);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }

        FileUtils.deleteQuietly(new File(fileName));
    }

    private void uploadDepivotAvro(Object[][] data, String avroDir, String fileName) {
        List<GenericRecord> records = new ArrayList<>();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Domain\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Topic1\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Score1\",\"type\":[\"double\",\"null\"]},"
                + "{\"name\":\"Topic2\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Score2\",\"type\":[\"double\",\"null\"]},"
                + "{\"name\":\"Topic3\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Score3\",\"type\":[\"double\",\"null\"]},"
                + "{\"name\":\"Topic4\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Score4\",\"type\":[\"double\",\"null\"]},"
                + "{\"name\":\"Timestamp\",\"type\":[\"long\",\"null\"]}" + "]}");
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple : data) {
            builder.set("Domain", tuple[0]);
            builder.set("Topic1", tuple[1]);
            builder.set("Score1", tuple[2]);
            builder.set("Topic2", tuple[3]);
            builder.set("Score2", tuple[4]);
            builder.set("Topic3", tuple[5]);
            builder.set("Score3", tuple[6]);
            builder.set("Topic4", tuple[7]);
            builder.set("Score4", tuple[8]);
            builder.set("Timestamp", tuple[9]);
            records.add(builder.build());
        }

        try {
            AvroUtils.writeToLocalFile(schema, records, fileName);
            if (HdfsUtils.fileExists(configuration, avroDir + "/" + fileName)) {
                HdfsUtils.rmdir(configuration, avroDir + "/" + fileName);
            }
            HdfsUtils.copyLocalToHdfs(configuration, fileName, avroDir + "/" + fileName);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }

        FileUtils.deleteQuietly(new File(fileName));
    }

}
