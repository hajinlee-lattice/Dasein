package com.latticeengines.propdata.dataflow.common;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.propdata.dataflow.CountFlowParameters;

@Component("countFlow")
public class CountFlow extends TypesafeDataFlowBuilder<CountFlowParameters> {

    public static final String COUNT = "Count";

    @Override
    public Node construct(CountFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        source = source.addRowID("ID");
        source = removeCountField(source);
        source = source.aggregate(new Aggregation("ID", COUNT, Aggregation.AggregationType.COUNT));
        return source;
    }

    private Node removeCountField(Node source) {
        List<String> fieldNames = new ArrayList<>();
        boolean hasCount = false;
        for (FieldMetadata fm : source.getSchema()) {
            if (fm.getFieldName().equals(COUNT)) {
                hasCount = true;
            } else {
                fieldNames.add(fm.getFieldName());
            }
        }

        if (hasCount) {
            source = source.retain(new FieldList(fieldNames.toArray(new String[fieldNames.size()])));
        }

        return source;
    }

}
