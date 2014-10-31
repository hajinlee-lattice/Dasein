package com.latticeengines.workflow.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:workflow-context.xml", "classpath:workflow-properties-context.xml" })
public class WorkflowServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String LEAD_TABLE = "lead";
    private static final String EVENT_TABLE_TABLE = "EventTable";

    private static final String CUSTOMER = "PropData-Pipeline-Tester";

    @Autowired
    private WorkflowService workflowService;

    @Test(groups = "functional", dataProvider = "workflowData")
    public void run(String customer, String table, boolean mapColumn, String keyCols) throws Exception {

        WorkflowContext workflowContext = getWorkflowContext(customer, table, mapColumn, keyCols);

        workflowService.run(workflowContext);

    }

    private WorkflowContext getWorkflowContext(String customer, String table, boolean mapColumn, String keyCols) {
        WorkflowContext workflowContext = new WorkflowContext();

        workflowContext.setProperty("customer", customer);
        workflowContext.setProperty("table", table);

        workflowContext.setProperty("propDataKeyCols", keyCols);
        workflowContext.setProperty("propDataMapColumn", mapColumn);
        workflowContext.setProperty("deploymentExternalID", customer);
        workflowContext.setProperty("contractExternalID", customer);
        workflowContext.setProperty("propDataCreatedBy", "propdata@lattice-engines.com");
        workflowContext.setProperty("propDataCommandName", "RunMatchWithLEUniverse");
        workflowContext.setProperty("propDataDestTables", "DerivedColumns|Alexa_Source");
        workflowContext.setProperty("propDataIsDownloading", Boolean.FALSE);
        workflowContext.setProperty("propDataMaxNumRetries", 5);

        return workflowContext;
    }

    @DataProvider(name = "workflowData")
    public Object[][] getWorkflowData() {
        return new Object[][] {
        // { CUSTOMER, LEAD_TABLE, false, "Email" },

        { CUSTOMER, EVENT_TABLE_TABLE, true, "RowId" }, };
    }

}
