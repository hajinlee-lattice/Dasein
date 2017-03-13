package com.latticeengines.dataflow.exposed.operation;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class RenameTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() throws Exception {
        uploadAvro();

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);
                Node renamed = node.rename(new FieldList("IntFieldA", "IntFieldB"), new FieldList("IntFieldA2", "IntFieldB2")).renamePipe("renamed");
                return node.join("IntFieldA", renamed, "IntFieldA2", JoinType.LEFT);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
        }

        cleanupDynamicSource();
    }

    private void uploadAvro() {
        Object[][] data = new Object[][] { //
                { 1, 2 }, //
                { 3, 4 } //
        };

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\"type\":\"record\",\"name\":\"Test\",\"doc\":\"Testing data\"," //
                + "\"fields\":[" //
                + "{\"name\":\"IntFieldA\",\"type\":[\"int\",\"null\"]}," //
                + "{\"name\":\"IntFieldB\",\"type\":[\"int\",\"null\"]}" //
                + "]}");

        cleanupDynamicSource();
        uploadDynamicSourceAvro(data, schema);
    }
}
