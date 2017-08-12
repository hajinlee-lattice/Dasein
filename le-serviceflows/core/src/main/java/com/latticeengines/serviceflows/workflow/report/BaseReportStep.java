package com.latticeengines.serviceflows.workflow.report;

import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.workflow.KeyValue;
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
        Report report = createReport(JsonUtils.serialize(json));
        registerReport(getConfiguration().getCustomerSpace(), report);
    }

    private Report createReport(String json) {
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        report.setJson(kv);
        report.setPurpose(getPurpose());
        String prefix = getNamePrefix();
        if (prefix != null && !prefix.isEmpty()) {
            report.setName(getNamePrefix() + "_" + UUID.randomUUID().toString());
        } else {
            report.setName(UUID.randomUUID().toString());
        }
        return report;
    }
}
