package com.latticeengines.dataflow.exposed.operation;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.query.ReferenceInterpretation;
import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

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
    public void testSimplePivot() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareSimplePivotData(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                Set<String> features = new HashSet<>(Arrays.asList("f1", "f2", "f3", "f4"));
                PivotStrategyImpl mapper = PivotStrategyImpl.pivotToClassWithDefaultValue(
                        "Feature", "Value", features, Integer.class, 0);
                return feature.pivot(new FieldList("Domain"), mapper);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record: output) {
            System.out.println(record);
            if (record.get("Domain").equals("dom1.com")) {
                Assert.assertEquals(record.get("f1"), 1);
                Assert.assertEquals(record.get("f2"), 2);
                Assert.assertEquals(record.get("f3"), 3);
                Assert.assertEquals(record.get("f4"), 0);
            } else if (record.get("Domain").equals("dom2.com")) {
                Assert.assertEquals(record.get("f1"), 0);
                Assert.assertEquals(record.get("f2"), 4);
                Assert.assertEquals(record.get("f3"), 2);
                Assert.assertEquals(record.get("f4"), 0);
            }
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }

    @Test(groups = "functional")
    public void testPivotWithPriority() throws Exception {
        String avroDir = "/tmp/avro/";
        String fileName = "Feature.avro";

        prepareSimplePivotData(avroDir, fileName);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource("Feature");
                Set<String> features = new HashSet<>(Arrays.asList("f1", "f2", "f3", "k1_low", "k1_high"));

                Map<String, Integer> priority = new HashMap<>();
                priority.put("k1_high", 2);

                Map<String, String> columns = new HashMap<>();
                columns.put("k1_low", "k1");
                columns.put("k1_high", "k1");

                PivotStrategyImpl mapper = new PivotStrategyImpl(
                        "Feature",
                        "Value",
                        features,
                        Integer.class,
                        columns,
                        priority,
                        null,
                        0,
                        1);

                return feature.pivot(new FieldList("Domain"), mapper);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record: output) {
            System.out.println(record);
            if (record.get("Domain").equals("dom1.com")) {
                Assert.assertEquals(record.get("f1"), 1);
                Assert.assertEquals(record.get("f2"), 2);
                Assert.assertEquals(record.get("f3"), 3);
                Assert.assertEquals(record.get("k1"), 5);
            } else if (record.get("Domain").equals("dom2.com")) {
                Assert.assertEquals(record.get("f1"), 0);
                Assert.assertEquals(record.get("f2"), 4);
                Assert.assertEquals(record.get("f3"), 2);
                Assert.assertEquals(record.get("k1"), 3);
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
        for (GenericRecord record: output) {
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
        for (GenericRecord record: output) {
            Long timestamp = (Long) record.get("New_Timestamp");
            Assert.assertNotNull(timestamp);
            Assert.assertTrue(timestamp > before);
            Assert.assertTrue(timestamp < after);
        }

        HdfsUtils.rmdir(configuration, avroDir + "." + fileName);
    }


    private void prepareSimplePivotData(String avroDir, String fileName) {
        Object[][] data = new Object[][] {
                {"dom1.com", "f1", 1, 123L},
                {"dom1.com", "f2", 2, 125L},
                {"dom1.com", "f3", 3, 124L},
                {"dom1.com", "k1_low", 3, 124L},
                {"dom1.com", "k1_high", 5, 124L},
                {"dom2.com", "f2", 4, 101L},
                {"dom2.com", "f3", 2, 102L},
                {"dom2.com", "k1_low", 3, 124L},
        };

        List<GenericRecord> records =  new ArrayList<>();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," +
                "\"fields\":[" +
                "{\"name\":\"Domain\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"Feature\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"Value\",\"type\":[\"int\",\"null\"]}," +
                "{\"name\":\"Timestamp\",\"type\":[\"long\",\"null\"]}" +
                "]}");
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple: data) {
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

}
