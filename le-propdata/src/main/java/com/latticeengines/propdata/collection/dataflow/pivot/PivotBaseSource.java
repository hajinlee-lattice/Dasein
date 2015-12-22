package com.latticeengines.propdata.collection.dataflow.pivot;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.pivot.PivotMapper;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;

@Component("pivotBaseSource")
public class PivotBaseSource extends TypesafeDataFlowBuilder<PivotDataFlowParameters> {

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        Node source = addSource(CollectionDataFlowKeys.SNAPSHOT_SOURCE);
        PivotMapper pivotMapper = parameters.getPivotMapper();
        FieldList groupbyField = parameters.getGroupbyFields();
        return source.pivot(groupbyField, pivotMapper);
    }



}
