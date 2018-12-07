package com.latticeengines.dataflow.exposed.operation;

import static com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy.KEY_ATTR;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.KVAttrPicker;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.KVDepivotStrategy;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class KVTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testKVDepivot() throws Exception {
        uploadAvro();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);
                return node.kvDepivot(new FieldList("RowId"), new FieldList("RowId", "Timestamp"));
            }
        });

        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), 25);
        for (GenericRecord record : output) {
            System.out.println(record);
            Assert.assertNull(record.get("Timestamp"));
            if (record.get(KEY_ATTR).equals("Attr1")) {
                if (new Long(5L).equals(record.get("RowId"))) {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(String.class)),
                            null);
                } else {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Boolean.class)),
                            String.valueOf(record.get("RowId")));
                }
            }
            if (record.get(KEY_ATTR).equals("Attr2")) {
                if (new Long(4L).equals(record.get("RowId"))) {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Double.class)),
                            null);
                } else {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Double.class)), 1.0);
                }
            }
            if (record.get(KEY_ATTR).equals("Attr3")) {
                if (new Long(3L).equals(record.get("RowId"))) {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Integer.class)),
                            null);
                } else {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Integer.class)), 1);
                }
            }
            if (record.get(KEY_ATTR).equals("Attr4")) {
                if (new Long(2L).equals(record.get("RowId"))) {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Boolean.class)),
                            null);
                } else {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Boolean.class)),
                            Boolean.TRUE);
                }
            }
            if (record.get(KEY_ATTR).equals("Attr5")) {
                if (new Long(5L).equals(record.get("RowId"))) {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(String.class)),
                            "only me");
                } else {
                    Assert.assertEquals(record.get(KVDepivotStrategy.valueAttr(Boolean.class)),
                            null);
                }
            }
        }

        cleanupDynamicSource();
    }

    @Test(groups = "functional")
    public void testKVReconstruct() throws Exception {
        uploadAvro();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);
                Node kv = node.kvDepivot(new FieldList("RowId"),
                        new FieldList("RowId", "Timestamp"));
                List<FieldMetadata> fms = Arrays.asList( //
                        new FieldMetadata("Attr5", String.class), //
                        new FieldMetadata("Attr4", Boolean.class), //
                        new FieldMetadata("Attr3", Integer.class), //
                        new FieldMetadata("Attr2", Double.class), //
                        new FieldMetadata("Attr1", String.class));
                return kv.kvReconstruct("RowId", fms);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().get(1).name(), "Attr5");
            Assert.assertEquals(record.getSchema().getFields().get(2).name(), "Attr4");
            Assert.assertEquals(record.getSchema().getFields().get(3).name(), "Attr3");
            Assert.assertEquals(record.getSchema().getFields().get(4).name(), "Attr2");
            Assert.assertEquals(record.getSchema().getFields().get(5).name(), "Attr1");
            if (new Long(1L).equals(record.get("RowId"))) {
                Assert.assertEquals(record.get("Attr1").toString(), "1");
            }
            if (new Long(5L).equals(record.get("RowId"))) {
                Assert.assertEquals(record.get("Attr5").toString(), "only me");
            }
        }
        cleanupDynamicSource();
    }

    @Test(groups = "functional")
    public void testTwoTableKV() throws Exception {
        uploadTwoTableAvro();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);

                // mimic two tables, or in real case, splitting an already join
                // table
                Node src1 = node
                        .rename(new FieldList("AttrA1", "AttrA2", "AttrA3", "AttrA4"),
                                new FieldList("Attr1", "Attr2", "Attr3", "Attr4")) //
                        .discard("AttrB1", "AttrB2", "AttrB3", "AttrB4");

                Node src2 = node
                        .rename(new FieldList("AttrB1", "AttrB2", "AttrB3", "AttrB4"),
                                new FieldList("Attr1", "Attr2", "Attr3", "Attr4")) //
                        .discard("AttrA1", "AttrA2", "AttrA3", "AttrA4");

                // depivot
                Node kv1 = src1.kvDepivot(new FieldList("RowId"), new FieldList("RowId"));
                Node kv2 = src2.kvDepivot(new FieldList("RowId"), new FieldList("RowId"));

                // mark origin
                kv1 = kv1.addColumnWithFixedValue("SourceTable", "Table1", String.class);
                kv2 = kv2.addColumnWithFixedValue("SourceTable", "Table2", String.class);

                // merge two streams
                Node kv = kv1.merge(kv2);

                // pick (discarding help fields is optional)
                Node attr1 = kv.kvPickAttr("RowId", new ExampleAttrPicker(String.class))
                        .renamePipe("attr1");
                Node attr2 = kv.kvPickAttr("RowId", new ExampleAttrPicker(Double.class))
                        .renamePipe("attr2");
                Node attr3 = kv.kvPickAttr("RowId", new ExampleAttrPicker(Integer.class))
                        .renamePipe("attr3");
                Node attr4 = kv.kvPickAttr("RowId", new ExampleAttrPicker(Boolean.class))
                        .renamePipe("attr4");
                // merge
                Node attr = attr1.merge(Arrays.asList(attr2, attr3, attr4));

                // reconstruct
                List<FieldMetadata> fms = Arrays.asList( //
                        new FieldMetadata("Attr1", String.class), //
                        new FieldMetadata("Attr2", Double.class), //
                        new FieldMetadata("Attr3", Integer.class), //
                        new FieldMetadata("Attr4", Boolean.class));
                return attr.kvReconstruct("RowId", fms);

            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
            switch (record.get("RowId").toString()) {
                case "2":
                    Assert.assertEquals(record.get("Attr1").toString(), "2");
                    break;
                case "3":
                    Assert.assertEquals(record.get("Attr2"), 2.0);
                    break;
                case "4":
                    Assert.assertEquals(record.get("Attr3"), 2);
                    break;
                case "5":
                    Assert.assertEquals(record.get("Attr4"), false);
                    break;
            }
        }
        cleanupDynamicSource();
    }

    private void uploadAvro() {
        long timestamp = System.currentTimeMillis();
        Object[][] data = new Object[][] { //
                { "1", 1.0, 1, true, null, 1L, timestamp }, //
                { "2", 1.0, 1, null, null, 2L, timestamp }, //
                { "3", 1.0, null, true, null, 3L, timestamp }, //
                { "4", null, 1, true, null, 4L, timestamp }, //
                { null, 1.0, 1, true, "only me", 5L, timestamp }, //
        };

        Schema.Parser parser = new Schema.Parser();
        // use single table mimic joined two tables: AttrA* from table A, AttrB*
        // from table B
        Schema schema = parser
                .parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," //
                        + "\"fields\":[" //
                        + "{\"name\":\"Attr1\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"Attr2\",\"type\":[\"double\",\"null\"]}," //
                        + "{\"name\":\"Attr3\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"Attr4\",\"type\":[\"boolean\",\"null\"]}," //
                        + "{\"name\":\"Attr5\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"RowId\",\"type\":[\"long\",\"null\"]}," //
                        + "{\"name\":\"Timestamp\",\"type\":[\"long\",\"null\"]}" //
                        + "]}");

        cleanupDynamicSource();
        uploadDynamicSourceAvro(data, schema);
    }

    private void uploadTwoTableAvro() {
        Object[][] data = new Object[][] { //
                { "1", 1.0, 1, true, "2", 2.0, 2, false, 1L }, // all from table
                                                               // 1
                { null, 1.0, 1, true, "2", 2.0, 2, false, 2L }, // attr1 from
                                                                // table 2
                { "1", null, 1, true, "2", 2.0, 2, false, 3L }, // attr2 from
                                                                // table 2
                { "1", 1.0, null, true, "2", 2.0, 2, false, 4L }, // attr3 from
                                                                  // table 2
                { "1", 1.0, 1, null, "2", 2.0, 2, false, 5L }, // attr4 from
                                                               // table 2
        };

        Schema.Parser parser = new Schema.Parser();
        // use single table mimic joined two tables: AttrA* from table A, AttrB*
        // from table B
        Schema schema = parser
                .parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," //
                        + "\"fields\":[" //
                        + "{\"name\":\"AttrA1\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"AttrA2\",\"type\":[\"double\",\"null\"]}," //
                        + "{\"name\":\"AttrA3\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"AttrA4\",\"type\":[\"boolean\",\"null\"]}," //
                        + "{\"name\":\"AttrB1\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"AttrB2\",\"type\":[\"double\",\"null\"]}," //
                        + "{\"name\":\"AttrB3\",\"type\":[\"int\",\"null\"]}," //
                        + "{\"name\":\"AttrB4\",\"type\":[\"boolean\",\"null\"]}," //
                        + "{\"name\":\"RowId\",\"type\":[\"long\",\"null\"]}" //
                        + "]}");

        cleanupDynamicSource();
        uploadDynamicSourceAvro(data, schema);
    }

    private static class ExampleAttrPicker implements KVAttrPicker {
        private static final long serialVersionUID = -5219012167374767129L;

        private static final Collection<String> HELP_FIELDS = Collections.singleton("SourceTable");

        private String valClzName;

        public ExampleAttrPicker(Class<?> valClz) {
            valClzName = valClz.getSimpleName();
        }

        @Override
        public Collection<String> helpFieldNames() {
            return HELP_FIELDS;
        }

        @Override
        public String valClzSimpleName() {
            return valClzName;
        }

        @Override
        public Object updateHelpAndReturnValue(Object oldValue, Map<String, Object> oldHelp,
                Object newValue, Map<String, Object> newHelp) {
            String newSource = (String) newHelp.get("SourceTable");
            boolean updateWithNewValue = false;
            if (oldValue == null) {
                // if candidate is null, update by new record anyway
                updateWithNewValue = true;
            } else if (newValue != null) {
                // if both candidate and new record are not null, check source
                // table
                updateWithNewValue = "Table1".equals(newSource);
            }

            if (updateWithNewValue) {
                oldHelp.put("SourceTable", newSource);
                return newValue;
            } else {
                return oldValue;
            }
        }
    }

}
