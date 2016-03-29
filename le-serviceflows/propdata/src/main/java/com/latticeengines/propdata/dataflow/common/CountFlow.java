package com.latticeengines.propdata.dataflow.common;

import java.util.UUID;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.propdata.dataflow.CountFlowParameters;

@Component("countFlow")
public class CountFlow extends TypesafeDataFlowBuilder<CountFlowParameters> {

    public static final String COUNT = "Count";
    public static final String ID = "CountID";

    @Override
    public Node construct(CountFlowParameters parameters) {
        String uid = UUID.randomUUID().toString();
        String idField = ID + uid;
        String countField = COUNT + uid;
        Node source = addSource(parameters.getSourceTable());
        source = source.addRowID(idField);
        source = source.aggregate(new Aggregation(idField, countField, AggregationType.COUNT));
        source = source.limit(1);
        source = source.retain(new FieldList(countField));
        source = source.rename(new FieldList(countField), new FieldList(COUNT));
        return source;
    }

}
