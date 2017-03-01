package com.latticeengines.dataflow.exposed.operation;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotType;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class PivotTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testSimplePivot() throws Exception {
        prepareSimplePivotData();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource(DYNAMIC_SOURCE);
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
    }

    @Test(groups = "functional")
    public void testMaxPivot() throws Exception {
        prepareMaxPivotData();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource(DYNAMIC_SOURCE);
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
    }

    @Test(groups = "functional")
    public void testCountPivot() throws Exception {
        prepareCountPivotData();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource(DYNAMIC_SOURCE);
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
    }

    @Test(groups = "functional")
    public void testExistsPivot() throws Exception {
        prepareMaxPivotData();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource(DYNAMIC_SOURCE);
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
    }

    @Test(groups = "functional")
    public void testMergingPivot() throws Exception {
        prepareMaxPivotData();

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
                Node feature = addSource(DYNAMIC_SOURCE);
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
    }

    static Schema featureSchema() {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Domain\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Feature\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Value\",\"type\":[\"int\",\"null\"]},"
                + "{\"name\":\"Timestamp\",\"type\":[\"long\",\"null\"]}" + "]}");
    }

    private void prepareSimplePivotData() {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 3, 124L }, { "dom1.com", "k1_low", 3, 124L }, { "dom1.com", "k1_high", 5, 124L },
                { "dom2.com", "f2", 4, 101L }, { "dom2.com", "f3", 2, 102L }, { "dom2.com", "k1_low", 3, 124L }, };
        cleanupDynamicSource();
        uploadDynamicSourceAvro(data, featureSchema());
    }

    private void prepareMaxPivotData() {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 4, 124L }, { "dom1.com", "f1", 2, 129L }, { "dom1.com", "f3", 1, 122L },
                { "dom1.com", "f2", 3, 121L }, { "dom1.com", "f2", 1, 122L }, };
        cleanupDynamicSource();
        uploadDynamicSourceAvro(data, featureSchema());
    }

    private void prepareCountPivotData() {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 4, 124L }, { "dom1.com", "f1", 2, 129L }, { "dom1.com", "f3", 1, 122L },
                { "dom1.com", "f2", 3, 121L }, { "dom1.com", "f2", 2, 122L }, { "dom2.com", "f1", 1, 123L },
                { "dom2.com", "f2", 2, 125L }, { "dom2.com", "f3", 4, 124L }, { "dom2.com", "f3", 1, 122L },
                { "dom2.com", "f2", 3, 121L }, { "dom2.com", "f2", 2, 122L } };
        cleanupDynamicSource();
        uploadDynamicSourceAvro(data, featureSchema());
    }

}
