package com.latticeengines.dataflow.exposed.operation;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class AddRowIdTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testAddRowId() throws IOException {
        int numRecords = 20;
        prepareAddRowId(numRecords);

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node feature = addSource(DYNAMIC_SOURCE);
                return feature.addRowID(new FieldMetadata("RowID", Long.class));
            }
        }, 4);

        List<GenericRecord> output = readOutput();
        Set<Long> ids = new HashSet<>();
        for (GenericRecord record : output) {
            System.out.println(record);
            Long thisId = (Long) record.get("RowID");
            ids.add(thisId);
        }
        Assert.assertEquals(ids.size(), numRecords * 2);

        cleanupDynamicSource();
    }

    private void prepareAddRowId(int numRecords) {

        Object[][] data = new Object[numRecords][4];
        for (int i = 0; i < numRecords; i++) {
            data[i] = new Object[] {  "data1", "data2"  };
        }

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," + "\"fields\":["
                + "{\"name\":\"Field1\",\"type\":[\"string\",\"null\"]},"
                + "{\"name\":\"Field2\",\"type\":[\"string\",\"null\"]}" + "]}");

        uploadDynamicSourceAvro(data, schema, "1.avro");
        uploadDynamicSourceAvro(data, schema, "2.avro");
    }

}
