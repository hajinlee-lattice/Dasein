package com.latticeengines.serviceflows.dataflow;

import java.util.Arrays;
import java.util.List;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.ParseMatchResultParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-core-dataflow-context.xml" })
public class ParseMatchResultRefineAndCloneTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional", enabled = false)
    public void test() {
        ParseMatchResultParameters parameters = new ParseMatchResultParameters();
        parameters.sourceTableName = "EventTable";
        parameters.sourceColumns = sourceCols();
        parameters.excludeDataCloudAttrs = true;
        executeDataFlow(parameters);
    }

    @Test(groups = "functional")
    public void testNotExclude() {
        ParseMatchResultParameters parameters = new ParseMatchResultParameters();
        parameters.sourceTableName = "EventTable";
        parameters.sourceColumns = sourceCols();
        parameters.excludeDataCloudAttrs = false;
        executeDataFlow(parameters);
    }

    private List<String> sourceCols() {
        return Arrays.asList("InternalId", "Id", "Email", "Event", "CompanyName", "City", "State", "Country",
                "CreatedDate", "FirstName", "LastName", "Title", "IsClosed", "StageName", "PhoneNumber", "Industry",
                "avro_1to300", "Activity_Count_Click_Email", "Activity_Count_Click_Link",
                "Activity_Count_Email_Bounced_Soft", "Activity_Count_Fill_Out_Form",
                "Activity_Count_Interesting_Moment_Any", "Activity_Count_Open_Email",
                "Activity_Count_Unsubscribe_Email", "Activity_Count_Visit_Webpage",
                "Activity_Count_Interesting_Moment_Email", "Activity_Count_Interesting_Moment_Event",
                "Activity_Count_Interesting_Moment_Pricing", "Activity_Count_Interesting_Moment_Webinar",
                "Activity_Count_Interesting_Moment_Multiple", "Activity_Count_Interesting_Moment_Search",
                "Activity_Count_Interesting_Moment_key_web_page", "Some_Column", "BusinessCountry", "SourceColumn");
    }

    @Override
    protected String getFlowBeanName() {
        return "parseMatchResult";
    }

    @Override
    protected String getScenarioName() {
        return "RefineAndClone";
    }
}
