package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component
public class ValidatePrematchEventTable extends BaseWorkflowStep<CreatePrematchEventTableReportConfiguration> {
    @Override
    public void execute() {
        Report report = retrieveReport(configuration.getCustomerSpace(), configuration.getReportName());
        ObjectNode json = JsonUtils.deserialize(report.getJson().getPayload(), ObjectNode.class);

        List<String> errors = new ArrayList<>();
        JsonNode count = json.get("count");
        JsonNode events = json.get("events");

        if (count.longValue() < configuration.getMinDedupedRows()) {
            errors.add(String
                    .format("Number of rows with unique domains (website, email address, etc...) must be greater than or equal to %d.  Found %d",
                            configuration.getMinDedupedRows(), count.longValue()));
        } else if (events.longValue() < configuration.getMinPositiveEvents()) {
            errors.add(String.format("Number of positive events must be greater than or equal to %d.  Found %d",
                    events.longValue(), configuration.getMinPositiveEvents()));
        }

        if (errors.size() > 0) {
            String message = StringUtils.join(errors, ";");
            throw new LedpException(LedpCode.LEDP_32000, new String[] { message });
        }
    }
}
