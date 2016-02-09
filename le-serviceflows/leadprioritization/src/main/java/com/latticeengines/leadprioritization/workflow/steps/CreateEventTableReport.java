package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;

@Component("createEventTableReport")
public class CreateEventTableReport extends BaseReportStep<BaseReportStepConfiguration> {
    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.EVENT_TABLE_IMPORT_SUMMARY;
    }

    @Override
    protected ObjectNode getJson() {
        // TODO
        ObjectNode json = new ObjectMapper().createObjectNode();
        json.put("count_missing_required_fields", 42);
        json.put("count_fields_malformed", 208);
        json.put("count_row_malformed", 402);
        return json;
    }
}
