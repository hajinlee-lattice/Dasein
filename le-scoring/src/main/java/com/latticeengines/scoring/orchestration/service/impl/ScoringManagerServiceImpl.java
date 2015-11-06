package com.latticeengines.scoring.orchestration.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.orchestration.service.ScoringManagerService;

@DisallowConcurrentExecution
@Component("scoringManagerService")
public class ScoringManagerServiceImpl extends QuartzJobBean implements ScoringManagerService {

    private static final Log log = LogFactory.getLog(ScoringManagerServiceImpl.class);

    private AsyncTaskExecutor scoringProcessorExecutor;

    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    private MetadataService metadataService;

    private Configuration yarnConfiguration;

    private JdbcTemplate scoringJdbcTemplate;

    private double cleanUpInterval;

    private String customerBaseDir;

    private boolean enableCleanHdfs;

    private int waitTime = 300;

    public void init(ApplicationContext appCtx) {
        scoringProcessorExecutor = (AsyncTaskExecutor) appCtx.getBean("scoringProcessorExecutor");
        scoringCommandEntityMgr = (ScoringCommandEntityMgr) appCtx.getBean("scoringCommandEntityMgr");
        scoringCommandResultEntityMgr = (ScoringCommandResultEntityMgr) appCtx.getBean("scoringCommandResultEntityMgr");
        metadataService = (MetadataService) appCtx.getBean("metadataService");
        scoringJdbcTemplate = (JdbcTemplate) appCtx.getBean("scoringJdbcTemplate");
        yarnConfiguration = (Configuration) appCtx.getBean("yarnConfiguration");
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        log.info("ScoringManager started!");
        SchedulerContext sc = null;
        try {
            sc = context.getScheduler().getContext();
        } catch (SchedulerException e) {
            log.error(e.getMessage(), e);
        }
        ApplicationContext appCtx = (ApplicationContext) sc.get("applicationContext");
        init(appCtx);
        List<Future<Long>> futures = new ArrayList<>();
        List<ScoringCommand> scoringCommands = scoringCommandEntityMgr.getPopulated();
        for (ScoringCommand scoringCommand : scoringCommands) {
            ScoringProcessorCallable sp = (ScoringProcessorCallable) appCtx.getBean("scoringProcessor");
            sp.setScoringCommand(scoringCommand);
            futures.add(scoringProcessorExecutor.submit(sp));
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
    }

    @VisibleForTesting
    void cleanTables() {
        List<ScoringCommand> consumedCommands = scoringCommandEntityMgr.getConsumed();
        DateTime dt = new DateTime(DateTimeZone.UTC);
        for (ScoringCommand scoringCommand : consumedCommands) {
            if (scoringCommand.getPopulated().getTime() + cleanUpInterval * 3600 * 1000 < dt.getMillis()) {
                metadataService.dropTable(scoringJdbcTemplate, scoringCommand.getTableName());
                if (enableCleanHdfs && scoringCommand.getConsumed() != null){
                    cleanHdfs(scoringCommand);
                }
                scoringCommandEntityMgr.delete(scoringCommand);
            }
        }

        List<ScoringCommandResult> consumedResultCommands = scoringCommandResultEntityMgr.getConsumed();
        for (ScoringCommandResult scoringCommandResult : consumedResultCommands) {
            if (scoringCommandResult.getConsumed().getTime() + cleanUpInterval * 3600 * 1000 < dt.getMillis()) {
                metadataService.dropTable(scoringJdbcTemplate, scoringCommandResult.getTableName());
                scoringCommandResultEntityMgr.delete(scoringCommandResult);
            }
        }
    }

    private void cleanHdfs(ScoringCommand scoringCommand) {
        try {
            String customer = scoringCommand.getId();
            HdfsUtils.rmdir(yarnConfiguration, customerBaseDir + "/" + CustomerSpace.parse(customer) + "/scoring/"
                    + scoringCommand.getTableName());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
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