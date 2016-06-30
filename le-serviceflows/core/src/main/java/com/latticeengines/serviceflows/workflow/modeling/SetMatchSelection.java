package com.latticeengines.serviceflows.workflow.modeling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Component("setMatchSelection")
public class SetMatchSelection extends BaseModelStep<ModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(SetMatchSelection.class);

    @Override
    public void execute() {
        log.info("Inside SetMatchSelection execute()");

        ModelSummary sourceSummary = configuration.getSourceModelSummary();
        if (sourceSummary != null) {
            try {
                String indented = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(sourceSummary);
                log.info("Found source model summary in configuration\n " + indented);
            } catch (Exception e) {
                // ignore
            }
            if (sourceSummary.getPredefinedSelection() != null) {
                executionContext.put(MATCH_PREDEFINED_SELECTION, sourceSummary.getPredefinedSelection());
                executionContext.put(MATCH_PREDEFINED_SELECTION_VERSION, sourceSummary.getPredefinedSelectionVersion());
            } else {
                executionContext.put(MATCH_CUSTOMIZED_SELECTION, sourceSummary.getCustomizedColumnSelection());
            }
        }
    }

}
