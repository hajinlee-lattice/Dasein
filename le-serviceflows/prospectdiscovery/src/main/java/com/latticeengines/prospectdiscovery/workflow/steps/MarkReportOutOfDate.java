package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.domain.exposed.pls.ReportPurpose;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("markReportOutOfDate")
public class MarkReportOutOfDate extends BaseWorkflowStep<TargetMarketStepConfiguration> {

    @Override
    public void execute() {
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        Report report = new Report();
        report.setPurpose(ReportPurpose.IMPORT_SUMMARY);
        report.setIsOutOfDate(true);
        proxy.registerReport(configuration.getTargetMarket().getName(), report, configuration.getCustomerSpace()
                .toString());
    }
}
