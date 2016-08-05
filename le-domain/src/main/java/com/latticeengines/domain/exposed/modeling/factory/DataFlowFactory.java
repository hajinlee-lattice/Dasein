package com.latticeengines.domain.exposed.modeling.factory;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class DataFlowFactory {

    private static final Log log = LogFactory.getLog(DataFlowFactory.class);

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
            TransformationGroup transformationGroup, Map<String, String> runTimeParams) {
        AddStandardAttributesParameters params = new AddStandardAttributesParameters(eventTableName, transformationGroup);
        
        if (runTimeParams != null && runTimeParams.containsKey(DATAFLOW_DO_SORT_FOR_ATTR_FLOW)) {
            params.doSort = true;
        } else {
            params.doSort = false;
        }
        return params;
    }
}
