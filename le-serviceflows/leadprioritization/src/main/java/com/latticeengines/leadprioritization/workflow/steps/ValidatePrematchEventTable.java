package com.latticeengines.leadprioritization.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CreatePrematchEventTableReportConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component
public class ValidatePrematchEventTable extends BaseWorkflowStep<CreatePrematchEventTableReportConfiguration> {
    @Override
    public void execute() {
        Report report = retrieveReport(configuration.getCustomerSpace(), ReportPurpose.PREMATCH_EVENT_TABLE_SUMMARY);
        ObjectNode json = JsonUtils.deserialize(report.getJson().getPayload(), ObjectNode.class);
        validate(json, configuration);
    }

    @VisibleForTesting
    void validate(JsonNode json, CreatePrematchEventTableReportConfiguration configuration) {
        List<String> errors = new ArrayList<>();
        JsonNode count = json.get("count");
        JsonNode events = json.get("events");

        if (count.longValue() < configuration.getMinRows()) {
            errors.add(String.format(
                    "Number of rows with unique domains (website, email address, etc...) must be greater than or equal to %d.  Found %d",
                    configuration.getMinRows(), count.longValue()));
        }
        if (events.longValue() < configuration.getMinPositiveEvents()) {
            errors.add(String.format("Number of positive events must be greater than or equal to %d.  Found %d",
                    configuration.getMinPositiveEvents(), events.longValue()));
        }

        if ((count.longValue() - events.longValue()) < configuration.getMinNegativeEvents()) {
            errors.add(String.format("Number of negative events must be greater than or equal to %d.  Found %d",
                    configuration.getMinNegativeEvents(), (count.longValue() - events.longValue())));
        }

        if (errors.size() > 0) {
            String message = StringUtils.join(errors, "\n");
            throw new LedpException(LedpCode.LEDP_32000, new String[] { message });
        }
    }
}
