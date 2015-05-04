package com.latticeengines.scoring.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.service.ScoringCommandLogService;
import com.latticeengines.scoring.service.ScoringManagerService;
import com.latticeengines.scoring.service.ScoringStepProcessor;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

@DisallowConcurrentExecution
@Component("scoringManagerService")
public class ScoringManagerServiceImpl extends QuartzJobBean implements ScoringManagerService {

    private static final Log log = LogFactory.getLog(ScoringManagerServiceImpl.class);

    private AsyncTaskExecutor scoringProcessorExecutor;

    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    private ScoringCommandLogService scoringCommandLogService;

    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    private ScoringStepProcessor scoringStepFinishProcessor;

    private JobService jobService;

    private MetadataService metadataService;

    private SqoopSyncJobService sqoopSyncJobService;

    private Configuration yarnConfiguration;

    private String appTimeLineWebAppAddress;

    private JdbcTemplate scoringJdbcTemplate;

    private double cleanUpInterval;

    private DbCreds scoringCreds;
    
    private String customerBaseDir;
    
    private boolean enableCleanHdfs;

    private int waitTime = 180;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        log.info("ScoringManager started!");
        log.info("look at database rows: " + scoringCommandEntityMgr.getPopulated());
        cleanTables();
        List<Future<Long>> futures = new ArrayList<>();
        List<ScoringCommand> scoringCommands = scoringCommandEntityMgr.getPopulated();
        for (ScoringCommand scoringCommand : scoringCommands) {
            futures.add(scoringProcessorExecutor.submit(new ScoringProcessorCallable(scoringCommand,
                    scoringCommandEntityMgr, scoringCommandLogService, scoringCommandStateEntityMgr,
                    scoringStepYarnProcessor, scoringStepFinishProcessor, jobService, yarnConfiguration,
                    appTimeLineWebAppAddress)));
        }

        for (Future<Long> future : futures) {
            try {
                Long pid = future.get(waitTime, TimeUnit.SECONDS);
                log.info("PId: " + pid);
            } catch (InterruptedException e) {
                log.error(ExceptionUtils.getStackTrace(e));
            } catch (ExecutionException e) {
                log.error(ExceptionUtils.getStackTrace(e));
            } catch (TimeoutException e) {
                log.error(ExceptionUtils.getStackTrace(e));
            }
        }
    }

    @VisibleForTesting
    void cleanTables() {
         List<ScoringCommand> consumedCommands = scoringCommandEntityMgr.getConsumed();
         for(ScoringCommand scoringCommand : consumedCommands){
             if (scoringCommand.getConsumed() != null && scoringCommand.getConsumed().getTime() + cleanUpInterval * 3600 * 1000 < System.currentTimeMillis()) {
                 metadataService.dropTable(scoringJdbcTemplate, scoringCommand.getTableName());
                 for(ScoringCommandState scoringCommandState : scoringCommandStateEntityMgr.findByScoringCommand(scoringCommand)){
                     scoringCommandStateEntityMgr.delete(scoringCommandState);
                 }
                 for(ScoringCommandLog scoringCommandLog : scoringCommandLogService.findByScoringCommand(scoringCommand)){
                     scoringCommandLogService.delete(scoringCommandLog);
                 }
                 if (enableCleanHdfs)
                     cleanHdfs(scoringCommand);
                 scoringCommandEntityMgr.delete(scoringCommand);
             }
         }

        List<ScoringCommandResult> consumedResultCommands = scoringCommandResultEntityMgr.getConsumed();
        for (ScoringCommandResult scoringCommandResult : consumedResultCommands) {
            if (scoringCommandResult.getConsumed().getTime() + cleanUpInterval < System.currentTimeMillis()) {
                metadataService.dropTable(scoringJdbcTemplate, scoringCommandResult.getTableName());
                scoringCommandResultEntityMgr.delete(scoringCommandResult);
            }
        }
    }

    private void cleanHdfs(ScoringCommand scoringCommand){
        try {
            HdfsUtils.rmdir(yarnConfiguration, customerBaseDir + "/" + scoringCommand.getId() + "/scoring/" + scoringCommand.getTableName());
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
    }

    public AsyncTaskExecutor getScoringProcessorExecutor() {
        return scoringProcessorExecutor;
    }

    public void setScoringProcessorExecutor(AsyncTaskExecutor scoringProcessorExecutor) {
        this.scoringProcessorExecutor = scoringProcessorExecutor;
    }

    public ScoringCommandEntityMgr getScoringCommandEntityMgr() {
        return scoringCommandEntityMgr;
    }

    public void setScoringCommandEntityMgr(ScoringCommandEntityMgr scoringCommandEntityMgr) {
        this.scoringCommandEntityMgr = scoringCommandEntityMgr;
    }

    public ScoringCommandLogService getScoringCommandLogService() {
        return this.scoringCommandLogService;
    }

    public void setScoringCommandLogService(ScoringCommandLogService scoringCommandLogService) {
        this.scoringCommandLogService = scoringCommandLogService;
    }

    public ScoringCommandStateEntityMgr getScoringCommandStateEntityMgr() {
        return this.scoringCommandStateEntityMgr;
    }

    public void setScoringCommandStateEntityMgr(ScoringCommandStateEntityMgr scoringCommandStateEntityMgr) {
        this.scoringCommandStateEntityMgr = scoringCommandStateEntityMgr;
    }

    public ScoringCommandResultEntityMgr getScoringCommandResultEntityMgr() {
        return scoringCommandResultEntityMgr;
    }

    public void setScoringCommandResultEntityMgr(ScoringCommandResultEntityMgr scoringCommandResultEntityMgr) {
        this.scoringCommandResultEntityMgr = scoringCommandResultEntityMgr;
    }

    public ScoringStepYarnProcessor getScoringStepYarnProcessor() {
        return this.scoringStepYarnProcessor;
    }

    public void setScoringStepYarnProcessor(ScoringStepYarnProcessor scoringStepYarnProcessor) {
        this.scoringStepYarnProcessor = scoringStepYarnProcessor;
    }

    public ScoringStepProcessor getScoringStepFinishProcessor() {
        return this.scoringStepFinishProcessor;
    }

    public void setScoringStepFinishProcessor(ScoringStepProcessor scoringStepFinishProcessor) {
        this.scoringStepFinishProcessor = scoringStepFinishProcessor;
    }

    public JobService getJobService() {
        return jobService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public void setMetadataService(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    public SqoopSyncJobService getSqoopSyncJobService() {
        return sqoopSyncJobService;
    }

    public void setSqoopSyncJobService(SqoopSyncJobService sqoopSyncJobService) {
        this.sqoopSyncJobService = sqoopSyncJobService;
    }

    public Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    public String getAppTimeLineWebAppAddress() {
        return appTimeLineWebAppAddress;
    }

    public void setAppTimeLineWebAppAddress(String appTimeLineWebAppAddress) {
        this.appTimeLineWebAppAddress = appTimeLineWebAppAddress;
    }

    public JdbcTemplate getScoringJdbcTemplate() {
        return scoringJdbcTemplate;
    }

    public void setScoringJdbcTemplate(JdbcTemplate scoringJdbcTemplate) {
        this.scoringJdbcTemplate = scoringJdbcTemplate;
    }

    public DbCreds getScoringCreds() {
        return scoringCreds;
    }

    public void setScoringCreds(DbCreds scoringCreds) {
        this.scoringCreds = scoringCreds;
    }

    public String getCustomerBaseDir() {
        return customerBaseDir;
    }

    public void setCustomerBaseDir(String customerBaseDir) {
        this.customerBaseDir = customerBaseDir;
    }

    public double getCleanUpInterval() {
        return cleanUpInterval;
    }

    public void setCleanUpInterval(double cleanUpInterval) {
        this.cleanUpInterval = cleanUpInterval;
    }

    public boolean getEnableCleanHdfs() {
        return enableCleanHdfs;
    }

    public void setEnableCleanHdfs(boolean enableCleanHdfs) {
        this.enableCleanHdfs = enableCleanHdfs;
    }

}