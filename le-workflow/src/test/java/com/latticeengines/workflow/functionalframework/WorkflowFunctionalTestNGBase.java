package com.latticeengines.workflow.functionalframework;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.TransformerUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.workflow.core.DataPlatformInfrastructure;

@ContextConfiguration(locations = { "classpath:test-workflow-context.xml" })
public class WorkflowFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private DataSource dataSource;

    protected JobRepositoryTestUtils jobRepositoryTestUtils;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setup() {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, dataSource);
        jobRepositoryTestUtils.setTablePrefix(DataPlatformInfrastructure.WORKFLOW_PREFIX);
    }

    @BeforeMethod(enabled = true, firstTimeOnly = true, alwaysRun = true)
    public void beforeEachTest() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    protected Set<String> getStepNamesFromExecution(JobExecution jobExecution) {
        @SuppressWarnings("unchecked")
        Collection<String> stepNames = CollectionUtils.collect(jobExecution.getStepExecutions(),
                TransformerUtils.invokerTransformer("getStepName"));

        return new HashSet<String>(stepNames);
    }


}
