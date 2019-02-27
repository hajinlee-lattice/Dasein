package com.latticeengines.workflowapi.flows.testflows.report;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("registerReport")
public class TestRegisterReport extends BaseReportStep<BaseReportStepConfiguration> {
    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.EMPLOYEE_ATTR_LEVEL_SUMMARY;
    }

    @Override
    protected ObjectNode getJson() {
        saveOutputValue("Some", "Output");

        ObjectNode node = new ObjectMapper().createObjectNode();
        node.put("foo", "bar");
        return node;
    }
}
