package com.latticeengines.serviceflows.workflow.modeling;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Component("setMatchSelection")
public class SetMatchSelection extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SetMatchSelection.class);

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
                putStringValueInContext(MATCH_PREDEFINED_SELECTION, sourceSummary.getPredefinedSelection().getName());
                putStringValueInContext(MATCH_PREDEFINED_SELECTION_VERSION, sourceSummary.getPredefinedSelectionVersion());
            } else {
                putObjectInContext(MATCH_CUSTOMIZED_SELECTION, sourceSummary.getCustomizedColumnSelection());
            }
        }
    }

}
