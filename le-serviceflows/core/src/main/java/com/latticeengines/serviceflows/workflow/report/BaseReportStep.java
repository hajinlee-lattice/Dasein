package com.latticeengines.serviceflows.workflow.report;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

/**
 * A base report generation step for generating a single report.
 */
public abstract class BaseReportStep<T extends BaseReportStepConfiguration> extends BaseWorkflowStep<T> {
    private String getName() {
        return getConfiguration().getReportName();
    }

    protected abstract ReportPurpose getPurpose();

    protected abstract ObjectNode getJson();

    @Override
    public final void execute() {
        ObjectNode json = getJson();
        if (json != null) {
            Report report = createReport(json.toString());

            registerReportInContext(report);

            InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(getConfiguration()
                    .getInternalResourceHostPort());
            proxy.registerReport(report, getConfiguration().getCustomerSpace().toString());
        }
    }

    private Report createReport(String json) {
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        report.setJson(kv);
        report.setPurpose(getPurpose());
        report.setName(getName());
        return report;
    }
}
