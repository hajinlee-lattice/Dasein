package com.latticeengines.workflow.core;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LEJobExecutionRetriever {

    private JobInstanceDao jobInstanceDao;

    private JobExecutionDao jobExecutionDao;

    private StepExecutionDao stepExecutionDao;

    @SuppressWarnings("unused")
    private ExecutionContextDao executionContextDao;

    @Bean
    public LEJobExecutionRetriever leJobExecutionRetriever() {
        return new LEJobExecutionRetriever();
    }

    public void setJobInstanceDao(JobInstanceDao jobInstanceDao){
        this.jobInstanceDao = jobInstanceDao;
    }

    public JobExecutionDao getJobExecutionDao() {
        return this.jobExecutionDao;
    }

    public void setJobExecutionDao(JobExecutionDao jobExecutionDao){
        this.jobExecutionDao = jobExecutionDao;
    }

    public StepExecutionDao getStepExecutionDao() {
        return this.stepExecutionDao;
    }

    public void setStepExecutionDao(StepExecutionDao stepExecutionDao){
        this.stepExecutionDao = stepExecutionDao;
    }

    public void setExecutionContextDao(ExecutionContextDao executionContextDao) {
        this.executionContextDao = executionContextDao;
    }

    public JobExecution getJobExecution(Long executionId) {
        return getJobExecution(executionId, true);
    }

    public JobExecution getJobExecution(Long executionId, Boolean includeJobSteps) {
        if (executionId == null) {
            return null;
        }
        JobExecution jobExecution = jobExecutionDao.getJobExecution(executionId);
        if (jobExecution == null) {
            return null;
        }
        JobInstance jobInstance = jobInstanceDao.getJobInstance(jobExecution);
        if (includeJobSteps) {
            stepExecutionDao.addStepExecutions(jobExecution);
        }
        jobExecution.setJobInstance(jobInstance);
        return jobExecution;
    }

    public ExecutionContext getExecutionContext(Long executionId) {
        if (executionId == null) {
            return null;
        }
        JobExecution jobExecution = jobExecutionDao.getJobExecution(executionId);
        if (jobExecution == null) {
            return null;
        }
        return executionContextDao.getExecutionContext(jobExecution);
    }
}
