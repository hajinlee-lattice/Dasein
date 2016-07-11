package com.latticeengines.domain.exposed.modeling.factory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.ModelingParameters;

public class DataFlowFactory {

    private static final Log log = LogFactory.getLog(DataFlowFactory.class);

    public static final String DATAFLOW_NAME_KEY = "dataflow.name";
    public static final String DATAFLOW_MATCH_KEY = "dataflow.match";

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
        if (dataFlow.getPredefinedSelectionName() != null) {
            parameters.setPredefinedSelectionName(dataFlow.getPredefinedSelectionName());
        }
        parameters.setExcludePropDataColumns(dataFlow.isExcludePropDataColumns());

        log.info("Successfully configured the DataFlow");
    }
}
