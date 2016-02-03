package com.latticeengines.propdata.dataflow.common;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.propdata.dataflow.CountFlowParameters;

@Component("countFlow")
public class CountFlow extends TypesafeDataFlowBuilder<CountFlowParameters> {

    public static final String COUNT = "Count";
    public static final String ID = "CountID";

    @Override
    public Node construct(CountFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        source = removeCountIdField(source);
        source = source.addRowID(ID);
        source = source.retain(new FieldList(ID));
        source = source.aggregate(new Aggregation(ID, COUNT, Aggregation.AggregationType.COUNT));
        source = source.retain(new FieldList(COUNT));
        return source;
    }

    private Node removeCountIdField(Node source) {
        List<String> fieldNames = new ArrayList<>();
        boolean hasCountId = false;
        for (FieldMetadata fm : source.getSchema()) {
            if (fm.getFieldName().equals(ID)) {
                hasCountId = true;
            } else {
                fieldNames.add(fm.getFieldName());
            }
        }

        if (hasCountId) {
            source = source.retain(new FieldList(fieldNames.toArray(new String[fieldNames.size()])));
        }

        return source;
    }

}
