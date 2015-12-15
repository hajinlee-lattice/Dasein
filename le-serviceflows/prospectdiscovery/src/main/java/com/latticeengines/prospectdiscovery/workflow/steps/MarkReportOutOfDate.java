package com.latticeengines.prospectdiscovery.workflow.steps;

import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.domain.exposed.pls.ReportPurpose;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;
import org.springframework.stereotype.Component;

@Component("markReportOutOfDate")
public class MarkReportOutOfDate extends BaseWorkflowStep<TargetMarketStepConfiguration> {

    @Override
    public void execute() {
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(configuration.getMicroServiceHostPort());
        Report report = new Report();
        report.setPurpose(ReportPurpose.IMPORT_SUMMARY);
        report.setIsOutOfDate(true);
        proxy.registerReport(configuration.getTargetMarket().getName(), report, configuration.getCustomerSpace()
                .toString());
    }
}
