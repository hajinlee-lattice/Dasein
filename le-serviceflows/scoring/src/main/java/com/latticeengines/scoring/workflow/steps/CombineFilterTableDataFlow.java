package com.latticeengines.scoring.workflow.steps;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineFilterTableParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineFilterTableConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("combineFilterTableDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CombineFilterTableDataFlow extends RunDataFlow<CombineFilterTableConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CombineFilterTableDataFlow.class);

    @Override
    public void onConfigurationInitialized() {
        if (StringUtils.isBlank(configuration.getTargetTableName())) {
            String targetTableName = NamingUtils.timestamp("CombinedFilters");
            configuration.setTargetTableName(targetTableName);
            log.info("Generated a new target table name: " + targetTableName);
        }
        configuration.setApplyTableProperties(true);
        configuration.setDataFlowParams(createDataFlowParameters());
    }

    private DataFlowParameters createDataFlowParameters() {
        String crossSellInputTable = getStringValueFromContext(FILTER_EVENT_TARGET_TABLE_NAME);
        String customEventInputTable = getStringValueFromContext(CUSTOM_EVENT_SCORE_FILTER_TABLENAME);
        if (StringUtils.isBlank(crossSellInputTable) && StringUtils.isBlank(customEventInputTable)) {
            throw new IllegalArgumentException("No input table for either CustomEvent or CrossSell");
        }
        CombineFilterTableParameters parameters = new CombineFilterTableParameters();
        parameters.crossSellInputTable = crossSellInputTable;
        parameters.customEventInputTable = customEventInputTable;
        return parameters;
    }

    @Override
    public void onExecutionCompleted() {
        putStringValueInContext(COMBINED_FILTER_TABLE_NAME, configuration.getTargetTableName());
    }

}
