package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.TargetMarketStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.InternalResourceRestApiProxy;

@Component("markReportOutOfDate")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MarkReportOutOfDate extends BaseWorkflowStep<TargetMarketStepConfiguration> {

    @Override
    public void execute() {
        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(
                configuration.getInternalResourceHostPort());
        Report report = new Report();
        report.setPurpose(ReportPurpose.IMPORT_SUMMARY);
        report.setIsOutOfDate(true);
        proxy.registerReport(configuration.getTargetMarket().getName(), report,
                configuration.getCustomerSpace().toString());
    }
}
