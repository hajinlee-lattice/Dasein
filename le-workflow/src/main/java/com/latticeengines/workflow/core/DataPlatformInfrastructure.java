package com.latticeengines.workflow.core;

import javax.sql.DataSource;

import org.hibernate.exception.LockAcquisitionException;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing(modular = true)
@Import({ AsyncInfrastructure.class, JobOperatorInfrastructure.class, LEJobExecutionRetriever.class })
public class DataPlatformInfrastructure implements BatchConfigurer {

    public static final String WORKFLOW_PREFIX = "WORKFLOW_";

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private TaskExecutor simpleAsyncTaskExecutor;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Value("${db.datasource.type}")
    private String databaseType;

    @Override
    public JobRepository getJobRepository() throws Exception {
        LEJobRepositoryFactoryBean factory = new LEJobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(getTransactionManager());
        factory.setDatabaseType(databaseType);
        factory.setIsolationLevelForCreate("ISOLATION_REPEATABLE_READ");
        factory.setTablePrefix(WORKFLOW_PREFIX);
        factory.setValidateTransactionState(false);
        factory.setMaxRetryAttempts(10);
        factory.addExceptionToRetry(DeadlockLoserDataAccessException.class);
        factory.addExceptionToRetry(DataAccessResourceFailureException.class);
        factory.addExceptionToRetry(LockAcquisitionException.class);
        factory.setRetryBackOffMultiplier(2.0);
        factory.setRetryBackOffInitialIntervalMsec(500);
        factory.setSerializer(new Jackson2ExecutionContextStringSerializer());
        factory.afterPropertiesSet();
        return (JobRepository) factory.getObject();
    }

    @Override
    public PlatformTransactionManager getTransactionManager() throws Exception {
        return transactionManager;
    }

    @Override
    public JobLauncher getJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(getJobRepository());
        jobLauncher.setTaskExecutor(simpleAsyncTaskExecutor);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() throws Exception {
        LEJobExplorerFactoryBean leJobExplorerFactoryBean = new LEJobExplorerFactoryBean();
        leJobExplorerFactoryBean.setDataSource(dataSource);
        leJobExplorerFactoryBean.setSerializer(new Jackson2ExecutionContextStringSerializer());
        leJobExplorerFactoryBean.setTablePrefix(WORKFLOW_PREFIX);
        leJobExplorerFactoryBean.setJobExecutionRetriever(leJobExecutionRetriever);
        leJobExplorerFactoryBean.afterPropertiesSet();
        return leJobExplorerFactoryBean.getObject();
    }

}
