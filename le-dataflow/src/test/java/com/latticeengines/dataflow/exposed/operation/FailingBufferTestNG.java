package com.latticeengines.dataflow.exposed.operation;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.dataflow.runtime.cascading.FailingBuffer;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

public class FailingBufferTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testDenormalize() throws Exception {
        Object[][] data = new Object[][] { //
                { "dom1.com", "x", 1, 123L }, //
                { "dom2.com", "y", 2, 123L }, //
                { "dom3.com", "z", 3, 123L }, //
                { "dom4.com", "b", 4, 124L }, //
                { "dom5.com", "a", 5, 124L }, //
        };
        uploadDynamicSourceAvro(data, PivotTestNG.featureSchema());

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);
                URL url = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/operation/ListType.avsc");
                String schemaStr;
                try {
                    schemaStr = FileUtils.readFileToString(new File(url.getFile()), Charset.defaultCharset());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Schema schema = new Schema.Parser().parse(schemaStr);
                List<FieldMetadata> fms = Arrays.asList(new FieldMetadata("ListFeature", List.class, schema), //
                        new FieldMetadata("Timestamp", Long.class));
                return node.groupByAndBuffer(new FieldList("Timestamp"), //
                        new FailingBuffer(new Fields("Timestamp", "ListFeature")), fms);
            }
        });
    }

}
