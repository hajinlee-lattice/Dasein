package com.latticeengines.scoring.orchestration.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;

public class ScoringManagerCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(ScoringManagerCallable.class);

    private AsyncTaskExecutor scoringProcessorExecutor;
    private ScoringCommandEntityMgr scoringCommandEntityMgr;
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;
    private MetadataService metadataService;
    private Configuration yarnConfiguration;
    private JdbcTemplate scoringJdbcTemplate;
    private ApplicationContext appContext;
    private double cleanUpInterval;
    private String customerBaseDir;
    private boolean enableCleanHdfs;
    private int waitTime = 300;

    public ScoringManagerCallable(Builder builder) {
        this.scoringProcessorExecutor = builder.getScoringProcessorExecutor();
        this.scoringCommandEntityMgr = builder.getScoringCommandEntityMgr();
        this.scoringCommandResultEntityMgr = builder.getScoringCommandResultEntityMgr();
        this.metadataService = builder.getMetadataService();
        this.yarnConfiguration = builder.getConfiguration();
        this.scoringJdbcTemplate = builder.getJdbcTemplate();
        this.appContext = builder.getApplicationContext();
        this.cleanUpInterval = builder.getCleanUpInterval();
        this.customerBaseDir = builder.getCustomerBaseDir();
        this.enableCleanHdfs = builder.getEnableCleanHdfs();
    }

    @Override
    public Boolean call() throws Exception {

        List<Future<Long>> futures = new ArrayList<>();
        List<ScoringCommand> scoringCommands = scoringCommandEntityMgr.getPopulated();
        for (ScoringCommand scoringCommand : scoringCommands) {
            ScoringProcessorCallable scoringProcessorCallable = (ScoringProcessorCallable) appContext.getBean("scoringProcessor");
            scoringProcessorCallable.setScoringCommand(scoringCommand);
            futures.add(scoringProcessorExecutor.submit(scoringProcessorCallable));
        }
        for (Future<Long> future : futures) {
            try {
                Long pid = future.get(waitTime, TimeUnit.SECONDS);
                log.info("PId: " + pid);
            } catch (Exception e) {
                log.fatal(e.getMessage(), e);
            }
        }
        cleanTables();
        return true;
    }

    @VisibleForTesting
    void cleanTables() {
        List<ScoringCommand> consumedCommands = scoringCommandEntityMgr.getConsumed();
        DateTime dt = new DateTime(DateTimeZone.UTC);
        for (ScoringCommand scoringCommand : consumedCommands) {
            if (scoringCommand.getPopulated().getTime() + cleanUpInterval * 3600 * 1000 < dt
                    .getMillis()) {
                metadataService.dropTable(scoringJdbcTemplate, scoringCommand.getTableName());
                if (enableCleanHdfs && scoringCommand.getConsumed() != null) {
                    cleanHdfs(scoringCommand);
                }
                scoringCommandEntityMgr.delete(scoringCommand);
            }
        }

        List<ScoringCommandResult> consumedResultCommands = scoringCommandResultEntityMgr
                .getConsumed();
        for (ScoringCommandResult scoringCommandResult : consumedResultCommands) {
            if (scoringCommandResult.getConsumed().getTime() + cleanUpInterval * 3600 * 1000 < dt
                    .getMillis()) {
                metadataService.dropTable(scoringJdbcTemplate, scoringCommandResult.getTableName());
                scoringCommandResultEntityMgr.delete(scoringCommandResult);
            }
        }
    }

    private void cleanHdfs(ScoringCommand scoringCommand) {
        try {
            String customer = scoringCommand.getId();
            HdfsUtils.rmdir(yarnConfiguration,
                    customerBaseDir + "/" + CustomerSpace.parse(customer) + "/scoring/"
                            + scoringCommand.getTableName());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static class Builder {

        private AsyncTaskExecutor scoringProcessorExecutor;
        private ScoringCommandEntityMgr scoringCommandEntityMgr;
        private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;
        private MetadataService metadataService;
        private Configuration yarnConfiguration;
        private JdbcTemplate scoringJdbcTemplate;
        private ApplicationContext appContext;
        private double cleanUpInterval;
        private String customerBaseDir;
        private boolean enableCleanHdfs;

        public Builder() {

        }

        public Builder scoringProcessorExecutor(AsyncTaskExecutor scoringProcessorExecutor) {
            this.scoringProcessorExecutor = scoringProcessorExecutor;
            return this;
        }

        public Builder scoringCommandEntityMgr(ScoringCommandEntityMgr scoringCommandEntityMgr) {
            this.scoringCommandEntityMgr = scoringCommandEntityMgr;
            return this;
        }

        public Builder scoringCommandResultEntityMgr(
                ScoringCommandResultEntityMgr scoringCommandResultEntityMgr) {
            this.scoringCommandResultEntityMgr = scoringCommandResultEntityMgr;
            return this;
        }

        public Builder metadataService(MetadataService metadataService) {
            this.metadataService = metadataService;
            return this;
        }

        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder scoringJdbcTemplate(JdbcTemplate scoringJdbcTemplate) {
            this.scoringJdbcTemplate = scoringJdbcTemplate;
            return this;
        }

        public Builder applicationContext(ApplicationContext appContext) {
            this.appContext = appContext;
            return this;
        }

        public Builder cleanUpInterval(double cleanUpInterval) {
            this.cleanUpInterval = cleanUpInterval;
            return this;
        }

        public Builder customerBaseDir(String customerBaseDir) {
            this.customerBaseDir = customerBaseDir;
            return this;
        }

        public Builder enableCleanHdfs(Boolean enableCleanHdfs) {
            this.enableCleanHdfs = enableCleanHdfs;
            return this;
        }

        public AsyncTaskExecutor getScoringProcessorExecutor() {
            return scoringProcessorExecutor;
        }

        public ScoringCommandEntityMgr getScoringCommandEntityMgr() {
            return scoringCommandEntityMgr;
        }

        public ScoringCommandResultEntityMgr getScoringCommandResultEntityMgr() {
            return scoringCommandResultEntityMgr;
        }

        public MetadataService getMetadataService() {
            return metadataService;
        }

        public Configuration getConfiguration() {
            return yarnConfiguration;
        }

        public JdbcTemplate getJdbcTemplate() {
            return scoringJdbcTemplate;
        }
        
        public ApplicationContext getApplicationContext() {
            return appContext;
        }

        public double getCleanUpInterval() {
            return cleanUpInterval;
        }

        public String getCustomerBaseDir() {
            return customerBaseDir;
        }

        public boolean getEnableCleanHdfs() {
            return enableCleanHdfs;
        }
    }

}
