package com.latticeengines.domain.exposed.modeling.factory;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

public class DataFlowFactory {

    private static final Logger log = LoggerFactory.getLogger(DataFlowFactory.class);

    public static final String DATAFLOW_NAME_KEY = "dataflow.name";
    public static final String DATAFLOW_MATCH_KEY = "dataflow.match";
    public static final String DATAFLOW_DO_SORT_FOR_ATTR_FLOW = "dataflow.do.sort";

    public static void configDataFlow(SelectedConfig config, ModelingParameters parameters) {
        log.info("Check and Config DataFlow.");
        if (config == null || config.getDataFlow() == null) {
            return;
        }
        DataFlow dataFlow = config.getDataFlow();
        if (dataFlow.getTransformationGroup() != null) {
            parameters.setTransformationGroup(dataFlow.getTransformationGroup());
        }
        if (dataFlow.getDedupType() != null) {
            parameters.setDeduplicationType(dataFlow.getDedupType());
        }

        log.info("Successfully configured the DataFlow");
    }

    public static AddStandardAttributesParameters getAddStandardAttributesParameters(String eventTableName, //
            List<TransformDefinition> transforms, Map<String, String> runTimeParams, String schema) {
        if (schema == null) {
            throw new RuntimeException("schema is null!");
        }
        AddStandardAttributesParameters params = new AddStandardAttributesParameters(eventTableName, transforms, SchemaInterpretation.valueOf(schema));

        if (runTimeParams != null && runTimeParams.containsKey(DATAFLOW_DO_SORT_FOR_ATTR_FLOW)) {
            params.doSort = true;
        } else {
            params.doSort = false;
        }
        return params;
    }
}
