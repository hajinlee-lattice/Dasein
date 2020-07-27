package com.latticeengines.domain.exposed.modeling.factory;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.AddStandardAttributesParameters;

public final class DataFlowFactory {

    protected DataFlowFactory() {
        throw new UnsupportedOperationException();
    }

    public static final String DATAFLOW_DO_SORT_FOR_ATTR_FLOW = "dataflow.do.sort";
    private static final Logger log = LoggerFactory.getLogger(DataFlowFactory.class);

    public static AddStandardAttributesParameters getAddStandardAttributesParameters(
            String eventTableName, //
            List<TransformDefinition> transforms, Map<String, String> runTimeParams,
            String schema) {
        if (schema == null) {
            throw new RuntimeException("schema is null!");
        }
        AddStandardAttributesParameters params = new AddStandardAttributesParameters(eventTableName,
                transforms, SchemaInterpretation.valueOf(schema));

        params.doSort = runTimeParams != null && runTimeParams.containsKey(DATAFLOW_DO_SORT_FOR_ATTR_FLOW);
        return params;
    }
}
