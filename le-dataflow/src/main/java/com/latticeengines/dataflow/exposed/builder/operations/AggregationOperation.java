package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.List;

import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AverageBy;
import cascading.pipe.assembly.CountBy;
import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class AggregationOperation extends Operation {
    public AggregationOperation(Input lhs, Aggregation aggregation) {
        Pipe lhsPipe = lhs.pipe;
        Pipe aggregationPipe = new Pipe(aggregation.getAggregationType() + "-" + lhsPipe.getName(), lhsPipe);
        Class<?> targetFieldClass = Double.class;
        switch (aggregation.getAggregationType()) {
        case AVG:
            aggregationPipe = new AverageBy(aggregationPipe, //
                    Fields.NONE, //
                    new Fields(aggregation.getAggregatedFieldName()), //
                    new Fields(aggregation.getTargetFieldName()));
            targetFieldClass = Double.class;
            break;
        case COUNT:
            aggregationPipe = new CountBy(aggregationPipe, //
                    Fields.NONE, //
                    new Fields(aggregation.getAggregatedFieldName()), //
                    new Fields(aggregation.getTargetFieldName()));
            targetFieldClass = Long.class;
        default:
            break;
        }
        List<FieldMetadata> mergedFields = new ArrayList<>();
        mergedFields.addAll(lhs.metadata);
        mergedFields.add(new FieldMetadata(aggregation.getTargetFieldName(), targetFieldClass));
        Pipe merged = new HashJoin(lhs + "-merged", lhsPipe, Fields.NONE, //
                aggregationPipe, Fields.NONE);
        this.pipe = merged;
        this.metadata = mergedFields;
    }

}
