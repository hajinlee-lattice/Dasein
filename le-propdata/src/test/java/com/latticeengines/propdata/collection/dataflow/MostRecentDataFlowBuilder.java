package com.latticeengines.propdata.collection.dataflow;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.propdata.collection.dataflow.buffer.MostRecentBuffer;

import cascading.operation.Buffer;
import cascading.tuple.Fields;

@Component("mostRecentDataFlow")
public class MostRecentDataFlowBuilder extends CascadingDataFlowBuilder {

    @Autowired
    YarnConfiguration yarnConfiguration;

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        for (Map.Entry<String, String> source: sources.entrySet()) {
            addSource(source.getKey(), source.getValue());
        }
        String[] sourceNames = sources.keySet().toArray(new String[sources.size()]);
        List<FieldMetadata> fms = getMetadata(sourceNames[0]);

        String[] fieldNames = new String[fms.size()];
        for (int i = 0; i < fms.size(); i++) {
            FieldMetadata metadata = fms.get(i);
            fieldNames[i] = metadata.getFieldName();
        }

        Buffer buffer = new MostRecentBuffer("Timestamp", new Fields(fieldNames));

        return addGroupByAndBuffer(sourceNames, new FieldList("Domain"), buffer, fms, true);
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }


}
