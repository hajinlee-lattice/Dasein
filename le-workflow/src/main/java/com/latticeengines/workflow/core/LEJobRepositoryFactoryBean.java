package com.latticeengines.workflow.core;

import static org.springframework.batch.support.DatabaseType.SYBASE;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.support.NameMatchMethodPointcut;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JdbcExecutionContextDao;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.support.DatabaseType;
import org.springframework.batch.support.PropertiesConverter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.latticeengines.common.exposed.retry.LoggingRetryOperationsInterceptor;

public class LEJobRepositoryFactoryBean extends JobRepositoryFactoryBean {
    private ProxyFactory proxyFactory;
    private String isolationLevelForCreate;
    private boolean validateTransactionState = true;
    private int maxRetryAttempts = 10;
    private Map<Class<? extends Throwable>, Boolean> exceptionsToRetry = new HashMap<>();
    private double retryBackOffMultiplier = 2.0;
    private long retryBackOffInitialIntervalMsec = 500;
    private JdbcTemplate jdbcTemplate;
    private String tablePrefix;
    private String databaseType;
    private LobHandler lobHandler;
    private Integer lobType;
    private int maxVarCharLength = AbstractJdbcBatchMetadataDao.DEFAULT_EXIT_MESSAGE_LENGTH;
    private ExecutionContextSerializer serializer;

    public LEJobRepositoryFactoryBean(JdbcTemplate jdbcTemplate, String tablePrefix, String databaseType,
            ExecutionContextSerializer serializer) {
        this.jdbcTemplate = jdbcTemplate;
        this.databaseType = databaseType;
        this.tablePrefix = tablePrefix;
        this.serializer = serializer;
        setJdbcOperations(jdbcTemplate);
        setDatabaseType(databaseType);
        setTablePrefix(tablePrefix);
        setSerializer(serializer);
    }

    private void initializeProxy() throws Exception {
        if (proxyFactory == null) {
            proxyFactory = new ProxyFactory();

            LoggingRetryOperationsInterceptor retryAdvice = new LoggingRetryOperationsInterceptor();
            RetryTemplate template = new RetryTemplate();
            SimpleRetryPolicy policy = new SimpleRetryPolicy(maxRetryAttempts, exceptionsToRetry, true);
            template.setThrowLastExceptionOnExhausted(true);
            template.setRetryPolicy(policy);

            ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
            backOffPolicy.setInitialInterval(retryBackOffInitialIntervalMsec);
            backOffPolicy.setMultiplier(retryBackOffMultiplier);
            template.setBackOffPolicy(backOffPolicy);

            retryAdvice.setRetryOperations(template);

            proxyFactory.addAdvice(retryAdvice);

            TransactionInterceptor transactionAdvice = new TransactionInterceptor(getTransactionManager(),
                    PropertiesConverter.stringToProperties("create*=PROPAGATION_REQUIRES_NEW," + isolationLevelForCreate
                            + "\ngetLastJobExecution*=PROPAGATION_REQUIRES_NEW," + isolationLevelForCreate
                            + "\n*=PROPAGATION_REQUIRED"));
            if (validateTransactionState) {
                DefaultPointcutAdvisor advisor = new DefaultPointcutAdvisor(new MethodInterceptor() {
                    @Override
                    public Object invoke(MethodInvocation invocation) throws Throwable {
                        if (TransactionSynchronizationManager.isActualTransactionActive()) {
                            throw new IllegalStateException("Existing transaction detected in JobRepository. "
                                    + "Please fix this and try again (e.g. remove @Transactional annotations from client).");
                        }
                        return invocation.proceed();
                    }
                });
                NameMatchMethodPointcut pointcut = new NameMatchMethodPointcut();
                pointcut.addMethodName("create*");
                advisor.setPointcut(pointcut);
                proxyFactory.addAdvisor(advisor);
            }
            proxyFactory.addAdvice(transactionAdvice);

            proxyFactory.setProxyTargetClass(false);
            proxyFactory.addInterface(JobRepository.class);
            proxyFactory.setTarget(getTarget());
        }
    }

    private Object getTarget() throws Exception {
        return new SimpleJobRepository(createJobInstanceDao(), createJobExecutionDao(), createStepExecutionDao(),
                createExecutionContextDao());
    }

    @Override
    public JobRepository getObject() throws Exception {
        initializeProxy();

        return (JobRepository) proxyFactory.getProxy();
    }

    @Override
    public void setIsolationLevelForCreate(String isolationLevelForCreate) {
        this.isolationLevelForCreate = isolationLevelForCreate;
    }

    @Override
    public void setValidateTransactionState(boolean validateTransactionState) {
        this.validateTransactionState = validateTransactionState;
    }

    public void setMaxRetryAttempts(int maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
    }

    public void setRetryBackOffInitialIntervalMsec(long retryBackOffInitialIntervalMsec) {
        this.retryBackOffInitialIntervalMsec = retryBackOffInitialIntervalMsec;
    }

    public void setRetryBackOffMultiplier(double retryBackOffMultiplier) {
        this.retryBackOffMultiplier = retryBackOffMultiplier;
    }

    public void addExceptionToRetry(Class<? extends Throwable> clazz) {
        exceptionsToRetry.put(clazz, true);
    }

    @Override
    protected ExecutionContextDao createExecutionContextDao() throws Exception {
        JdbcExecutionContextDao dao = new Utf8JdbcExecutionContextDao();
        dao.setJdbcTemplate(jdbcTemplate);
        dao.setTablePrefix(tablePrefix);
        dao.setClobTypeToUse(determineClobTypeToUse(databaseType));
        dao.setSerializer(serializer);

        if (lobHandler != null) {
            dao.setLobHandler(lobHandler);
        }

        dao.afterPropertiesSet();
        // Assume the same length.
        dao.setShortContextLength(maxVarCharLength);
        return dao;
    }

    private int determineClobTypeToUse(String databaseType) throws Exception {
        if (lobType != null) {
            return lobType;
        } else {
            if (SYBASE == DatabaseType.valueOf(databaseType.toUpperCase())) {
                return Types.LONGVARCHAR;
            } else {
                return Types.CLOB;
            }
        }
    }
}
