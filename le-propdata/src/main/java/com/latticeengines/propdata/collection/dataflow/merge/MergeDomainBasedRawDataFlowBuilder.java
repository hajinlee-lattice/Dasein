package com.latticeengines.propdata.collection.dataflow.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.propdata.collection.dataflow.buffer.MostRecentBuffer;
import com.latticeengines.propdata.collection.dataflow.function.DomainCleanupFunction;

import cascading.operation.Buffer;
import cascading.tuple.Fields;

public abstract class MergeDomainBasedRawDataFlowBuilder extends CascadingDataFlowBuilder {

    @Autowired
    YarnConfiguration yarnConfiguration;

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        List<String> cleanSources = new ArrayList<>();
        for (Map.Entry<String, String> source: sources.entrySet()) {
            addSource(source.getKey(), source.getValue());
            String cleanSource= addFunction(source.getKey(), new DomainCleanupFunction(domainField()),
                    new FieldList(domainField()), new FieldMetadata(domainField(), String.class));
            cleanSources.add(cleanSource);
        }
        String[] sourceNames = cleanSources.toArray(new String[sources.size()]);

        List<FieldMetadata> fms = getMetadata(sourceNames[0]);
        String[] fieldNames = new String[fms.size()];
        for (int i = 0; i < fms.size(); i++) {
            FieldMetadata metadata = fms.get(i);
            fieldNames[i] = metadata.getFieldName();
        }
        
        @SuppressWarnings("rawtypes")
        Buffer buffer = new MostRecentBuffer(timestampField(), new Fields(fieldNames));

        return addGroupByAndBuffer(sourceNames, new FieldList(uniqueFields()), buffer, fms, true);
    }

    @Override
    public Node constructFlowDefinition(DataFlowParameters parameters) {
        throw new IllegalStateException("Not supported");
    }

    protected abstract String domainField();

    protected abstract String timestampField();

    protected abstract String[] uniqueFields();

}
