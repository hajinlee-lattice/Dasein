package com.latticeengines.workflow.functionalframework;

import javax.sql.DataSource;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.workflow.core.DataPlatformInfrastructure;
import com.latticeengines.workflow.exposed.service.WorkflowService;

@ContextConfiguration(locations = { "classpath:test-workflow-context.xml" })
public class WorkflowFunctionalTestNGBase extends SecurityFunctionalTestNGBase {

    protected static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 5;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private DataSource dataSource;

    @Autowired
    protected WorkflowService workflowService;

    protected JobRepositoryTestUtils jobRepositoryTestUtils;

    protected boolean enableJobRepositoryCleanupBeforeTest() {
        // TODO set this back to true for jenkins
        return false;
    }

    @BeforeClass(groups = { "functional", "deployment" })
    public void beforeEachClass() {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, dataSource);
        jobRepositoryTestUtils.setTablePrefix(DataPlatformInfrastructure.WORKFLOW_PREFIX);
    }

    @BeforeMethod(enabled = true, firstTimeOnly = true, alwaysRun = true)
    public void beforeEachTest() {
        if (enableJobRepositoryCleanupBeforeTest()) {
            jobRepositoryTestUtils.removeJobExecutions();
        }
    }

}
