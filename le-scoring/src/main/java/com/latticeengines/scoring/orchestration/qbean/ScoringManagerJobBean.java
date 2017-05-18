package com.latticeengines.scoring.orchestration.qbean;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.orchestration.service.impl.ScoringManagerCallable;

@Component("scoringManagerJob")
public class ScoringManagerJobBean implements QuartzJobBean {

    @Autowired
    @Qualifier("commonTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private DbMetadataService dbMetadataService;

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

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        ScoringManagerCallable.Builder builder = new ScoringManagerCallable.Builder();
        builder.cleanUpInterval(cleanUpInterval)
                .customerBaseDir(customerBaseDir)
                .enableCleanHdfs(enableCleanHdfs)
                .metadataService(dbMetadataService)
                .scoringCommandEntityMgr(scoringCommandEntityMgr)
                .scoringCommandResultEntityMgr(scoringCommandResultEntityMgr)
                .scoringJdbcTemplate(scoringJdbcTemplate)
                .scoringProcessorExecutor(taskExecutor)
                .yarnConfiguration(yarnConfiguration)
                .applicationContext(appCtx);

        return new ScoringManagerCallable(builder);
    }

}
