package com.latticeengines.workflow.core;

import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.core.repository.dao.JdbcExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.jdbc.core.JdbcTemplate;

public class LEJobExplorerFactoryBean extends JobExplorerFactoryBean {

    private LEJobExecutionRetriever leJobExecutionRetriever;

    private JdbcTemplate jdbcTemplate;

    public LEJobExplorerFactoryBean(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setJobExecutionRetriever(LEJobExecutionRetriever leJobExecutionRetriever) {
        this.leJobExecutionRetriever = leJobExecutionRetriever;
    }

    @Override
    protected ExecutionContextDao createExecutionContextDao() throws Exception {
        JdbcExecutionContextDao dao = new Uft8JdbcExecutionContextDao();
        dao.setJdbcTemplate(jdbcTemplate);
        dao.setLobHandler(null);
        dao.setTablePrefix(AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX);
        dao.setSerializer(new Jackson2ExecutionContextStringSerializer());
        dao.afterPropertiesSet();
        return dao;
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
