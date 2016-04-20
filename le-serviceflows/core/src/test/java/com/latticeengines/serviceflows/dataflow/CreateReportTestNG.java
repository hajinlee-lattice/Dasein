package com.latticeengines.serviceflows.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.domain.exposed.dataflow.GroupByDomainReportColumn;
import com.latticeengines.domain.exposed.dataflow.ReportColumn;
import com.latticeengines.domain.exposed.dataflow.flows.CreateReportParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-core-context.xml" })
public class CreateReportTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    @Test(groups = "functional")
    public void test() {
        CreateReportParameters parameters = new CreateReportParameters();
        parameters.sourceTableName = "EventTable";
        parameters.columns.add(new GroupByDomainReportColumn("unique_domains", AggregationType.COUNT.toString()));
        parameters.columns.add(new ReportColumn("count", AggregationType.COUNT.toString()));
        executeDataFlow(parameters);
    }

    @Override
    protected String getFlowBeanName() {
        return "createReport";
    }
}
