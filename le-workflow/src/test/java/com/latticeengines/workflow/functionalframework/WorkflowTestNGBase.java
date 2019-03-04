package com.latticeengines.workflow.functionalframework;

import javax.sql.DataSource;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.workflow.core.DataPlatformInfrastructure;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;

@ContextConfiguration(locations = { "classpath:test-workflow-context.xml" })
public class WorkflowTestNGBase extends SecurityFunctionalTestNGBase {

    protected static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 5;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private DataSource dataSource;

    @Autowired
    protected WorkflowService workflowService;

    @Autowired
    protected WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    protected WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Autowired
    private TenantService tenantService;

    protected JobRepositoryTestUtils jobRepositoryTestUtils;

    protected static final String WORKFLOW_TENANT = "Workflow_Tenant";

    protected boolean enableJobRepositoryCleanupBeforeTest() {
        // TODO set this back to true for jenkins
        return false;
    }

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() throws Exception {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, dataSource);
        jobRepositoryTestUtils.setTablePrefix(DataPlatformInfrastructure.WORKFLOW_PREFIX);
        bootstrapWorkFlowTenant();
    }

    @BeforeMethod(enabled = true, firstTimeOnly = true, alwaysRun = true)
    public void beforeEachTest() {
        if (enableJobRepositoryCleanupBeforeTest()) {
            jobRepositoryTestUtils.removeJobExecutions();
        }
    }

    protected CustomerSpace bootstrapWorkFlowTenant() {
        CustomerSpace customerSpace = CustomerSpace.parse(WORKFLOW_TENANT);
        Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        Tenant tenant1 = new Tenant();
        tenant1.setId(customerSpace.toString());
        tenant1.setName(customerSpace.toString());
        tenantService.registerTenant(tenant1);
        MultiTenantContext.setTenant(tenant1);
        return customerSpace;
    }

    protected void cleanup(Long workflowJobId) {
        MultiTenantContext.setTenant(null);
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowJobId);
        if (workflowJob != null) {
            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
            if (jobUpdate != null) {
                workflowJobUpdateEntityMgr.delete(jobUpdate);
            }
            workflowJobEntityMgr.delete(workflowJob);
        }
    }
}
