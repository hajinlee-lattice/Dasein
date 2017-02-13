package com.latticeengines.dataflow.exposed.operation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class DepivotTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testDepivot() throws Exception {
        uploadDepivotAvro();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);
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

        cleanupDyanmicSource();
    }

    private void uploadDepivotAvro() {
        Object[][] data = new Object[][] {
                { "dom1.com", "topic1", 1.0, "topic2", 2.0, "topic3", 3.0, "topic4", 4.0, 123L },
                { "dom1.com", "topic5", 1.1, "topic6", 2.1, "topic7", null, null, 4.1, 123L } };

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

        cleanupDyanmicSource();
        uploadDynamicSourceAvro(data, schema);
    }

}
