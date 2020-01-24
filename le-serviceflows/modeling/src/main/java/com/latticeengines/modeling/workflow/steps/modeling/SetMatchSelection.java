package com.latticeengines.modeling.workflow.steps.modeling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;

@Component("setMatchSelection")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
            } catch (JsonProcessingException e) {
                log.warn("Failed to print source summary", e);
            }
            if (sourceSummary.getPredefinedSelection() != null) {
                putStringValueInContext(MATCH_PREDEFINED_SELECTION, sourceSummary.getPredefinedSelection().getName());
                putStringValueInContext(MATCH_PREDEFINED_SELECTION_VERSION,
                        sourceSummary.getPredefinedSelectionVersion());
            } else {
                putObjectInContext(MATCH_CUSTOMIZED_SELECTION, sourceSummary.getCustomizedColumnSelection());
            }
        }
    }

}
