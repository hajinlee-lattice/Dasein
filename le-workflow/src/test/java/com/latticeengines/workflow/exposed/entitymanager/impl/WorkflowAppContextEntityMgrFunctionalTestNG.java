package com.latticeengines.workflow.exposed.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowAppContext;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.workflow.exposed.entitymgr.WorkflowAppContextEntityMgr;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

public class WorkflowAppContextEntityMgrFunctionalTestNG extends WorkflowFunctionalTestNGBase {

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private WorkflowAppContextEntityMgr workflowAppContextEntityMgr;

    @Test(groups = "functional")
    public void testFindWorkflowIdsByTenant() throws Exception {
        Tenant tenantA = new Tenant();
        tenantA.setName("A.WorkflowAppContextEntityMgrFunctionalTestNG");
        tenantA.setId("A.WorkflowAppContextEntityMgrFunctionalTestNG");
        tenantEntityMgr.create(tenantA);
        Tenant tenantB = new Tenant();
        tenantB.setName("B.WorkflowAppContextEntityMgrFunctionalTestNG");
        tenantB.setId("B.WorkflowAppContextEntityMgrFunctionalTestNG");
        tenantEntityMgr.create(tenantB);

        try {
            for (long i = 1; i < 5; i++) {
                WorkflowAppContext workflowAppContext = new WorkflowAppContext();
                workflowAppContext.setWorkflowId(i);
                workflowAppContext.setTenant(tenantA);
                workflowAppContextEntityMgr.create(workflowAppContext);
            }

            for (long i = 5; i < 9; i++) {
                WorkflowAppContext workflowAppContext = new WorkflowAppContext();
                workflowAppContext.setWorkflowId(i);
                workflowAppContext.setTenant(tenantB);
                workflowAppContextEntityMgr.create(workflowAppContext);
            }

            List<WorkflowAppContext> workflowAppContexts = workflowAppContextEntityMgr.findWorkflowIdsByTenant(tenantA);
            assertEquals(workflowAppContexts.size(), 4);
        } finally {
            workflowAppContextEntityMgr.deleteAll();
            tenantEntityMgr.delete(tenantA);
            tenantEntityMgr.delete(tenantB);
        }
    }

}
