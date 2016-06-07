package com.latticeengines.scoring.orchestration.mbean;

import java.util.concurrent.Callable;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.orchestration.service.impl.ScoringManagerCallable;

@Component("scoringManagerJob")
public class ScoringManagerJobBean implements QuartzJobBean {

    private AsyncTaskExecutor scoringProcessorExecutor;

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Value("#{T(java.lang.Double).parseDouble(${scoring.cleanup.timeinternval})}")
    private double cleanUpInterval;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("#{T(java.lang.Boolean).parseBoolean(${scoring.cleanup.enableCleanHdfs})}")
    private boolean enableCleanHdfs;

    @Autowired
    private ApplicationContext appCtx;

    @Value("${scoring.max.pool.size}")
    private int maxPoolSize;

    @Value("${scoring.core.pool.size}")
    private int corePoolSize;

    @Value("${scoring.queue.capacity}")
    private int queueCapacity;

    @PostConstruct
    public void init() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(maxPoolSize);
        executor.setCorePoolSize(corePoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.initialize();
        scoringProcessorExecutor = executor;
    }

    @Override
    public Callable<Boolean> getCallable() {
//        ScoringProcessorCallable scoringProcessorCallable = (ScoringProcessorCallable) appCtx
//                .getBean("scoringProcessor");

        ScoringManagerCallable.Builder builder = new ScoringManagerCallable.Builder();
        builder.cleanUpInterval(cleanUpInterval)
                .customerBaseDir(customerBaseDir)
                .enableCleanHdfs(enableCleanHdfs)
                .metadataService(metadataService)
                .scoringCommandEntityMgr(scoringCommandEntityMgr)
                .scoringCommandResultEntityMgr(scoringCommandResultEntityMgr)
                .scoringJdbcTemplate(scoringJdbcTemplate)
                .scoringProcessorExecutor(scoringProcessorExecutor)
                .yarnConfiguration(yarnConfiguration)
                .applicationContext(appCtx);
                //.scoringProcessorCallable(scoringProcessorCallable);

        return new ScoringManagerCallable(builder);
    }

}
