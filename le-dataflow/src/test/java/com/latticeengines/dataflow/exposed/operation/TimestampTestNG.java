package com.latticeengines.dataflow.exposed.operation;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class TimestampTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testSwapTimestamp() throws Exception {
        prepareTimestampData();

        Long before = System.currentTimeMillis();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource(DYNAMIC_SOURCE);
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
    }

    @Test(groups = "functional")
    public void testAddTimestamp() throws Exception {

        prepareTimestampData();
        Long before = System.currentTimeMillis();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource(DYNAMIC_SOURCE);
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
    }

    private void prepareTimestampData() {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, { "dom1.com", "f2", 2, 125L },
                { "dom1.com", "f3", 3, 124L }, { "dom1.com", "k1_low", 3, 124L }, { "dom1.com", "k1_high", 5, 124L },
                { "dom2.com", "f2", 4, 101L }, { "dom2.com", "f3", 2, 102L }, { "dom2.com", "k1_low", 3, 124L }, };
        cleanupDynamicSource();
        uploadDynamicSourceAvro(data, PivotTestNG.featureSchema());
    }

}
