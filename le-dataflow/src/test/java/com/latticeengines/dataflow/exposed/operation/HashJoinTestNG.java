package com.latticeengines.dataflow.exposed.operation;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class HashJoinTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() throws Exception {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, //
                { "dom1.com", "f2", 2, 125L }, //
                { "dom1.com", "f3", 3, 124L }, //
                { "dom2.com", "f2", 4, 101L }, //
                { "dom2.com", "f3", 2, 102L } };
        uploadDynamicSourceAvro(data, PivotTestNG.featureSchema());

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);
                Node node1 = node.filter("Domain.equals(\"dom1.com\")", new FieldList("Domain")).renamePipe("pipe1");
                Node node2 = node.filter("Domain.equals(\"dom1.com\")", new FieldList("Domain")).renamePipe("pipe2");
                return node.hashJoin(new FieldList("Domain"), Arrays.asList(node1, node2), //
                        Arrays.asList(new FieldList("Domain"), new FieldList("Domain")), //
                        JoinType.INNER);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
        }
    }

}
