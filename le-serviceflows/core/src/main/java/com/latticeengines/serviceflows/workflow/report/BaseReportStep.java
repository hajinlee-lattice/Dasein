package com.latticeengines.serviceflows.workflow.report;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

/**
 * A base report generation step for generating a single report.
 */
public abstract class BaseReportStep<T extends BaseReportStepConfiguration> extends BaseWorkflowStep<T> {
    private String getNamePrefix() {
        return getConfiguration().getReportNamePrefix();
    }

    private ObjectNode json = JsonUtils.createObjectNode();

    protected abstract ReportPurpose getPurpose();

    protected ObjectNode getJson() {
        return json;
    }

    @Override
    public void execute() {
        ObjectNode json = getJson();
        String reportName = StringUtils.isNotEmpty(getNamePrefix())
                ? getNamePrefix() + "_" + UUID.randomUUID().toString() : UUID.randomUUID().toString();
        Report report = createReport(json.toString(), getPurpose(), reportName);
        registerReport(getConfiguration().getCustomerSpace(), report);
    }
}
