package com.latticeengines.workflow.core;

import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;

public class LEJobExplorerFactoryBean extends JobExplorerFactoryBean {

    private LEJobExecutionRetriever leJobExecutionRetriever;

    public void setJobExecutionRetriever(LEJobExecutionRetriever leJobExecutionRetriever) {
        this.leJobExecutionRetriever = leJobExecutionRetriever;
    }

    @Override
    public JobExplorer getObject() throws Exception {
        JobInstanceDao jobInstanceDao = createJobInstanceDao();
        JobExecutionDao jobExecutionDao = createJobExecutionDao();
        StepExecutionDao stepExecutionDao = createStepExecutionDao();
        ExecutionContextDao executionContextDao = createExecutionContextDao();
        leJobExecutionRetriever.setJobInstanceDao(jobInstanceDao);
        leJobExecutionRetriever.setJobExecutionDao(jobExecutionDao);
        leJobExecutionRetriever.setStepExecutionDao(stepExecutionDao);
        leJobExecutionRetriever.setExecutionContextDao(executionContextDao);
        return new SimpleJobExplorer(jobInstanceDao, jobExecutionDao, stepExecutionDao, executionContextDao);
    }

}
