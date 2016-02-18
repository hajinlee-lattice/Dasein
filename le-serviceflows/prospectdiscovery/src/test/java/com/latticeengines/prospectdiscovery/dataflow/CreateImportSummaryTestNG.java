package com.latticeengines.prospectdiscovery.dataflow;

import java.util.List;

import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;
import junit.framework.Assert;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class CreateImportSummaryTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        executeDataFlow();

        List<GenericRecord> output = readOutput();
        List<GenericRecord> accounts = readInput("Account");
        List<GenericRecord> opportunities = readInput("Opportunity");
        List<GenericRecord> leads = readInput("Lead");
        List<GenericRecord> contacts = readInput("Contact");
        GenericRecord stats = output.get(0);
        Assert.assertNotNull(stats);
        // There's some annoying issue with equality and the records in the avro
        // that requires explicit boxing to Long.
        Assert.assertEquals(new Long(accounts.size()), stats.get("TotalAccounts"));
        Assert.assertEquals(new Long(opportunities.size()), stats.get("TotalOpportunities"));
        Assert.assertEquals(new Long(leads.size()), stats.get("TotalLeads"));
        Assert.assertEquals(new Long(contacts.size()), stats.get("TotalContacts"));
        Assert.assertEquals(new Long(22), stats.get("TotalClosedWonOpportunities"));
    }

    @Override
    protected String getFlowBeanName() {
        return "createImportSummary";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return "CreatedDate";
    }
}
