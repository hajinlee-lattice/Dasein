package com.latticeengines.prospectdiscovery.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class CalculateEventTableStatisticsTestNG extends ServiceFlowsFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        Table result = executeDataFlow();
    }

    @Override
    protected String getFlowBeanName() {
        return "calculateEventTableStatistics";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return "CreatedDate";
    }
}
