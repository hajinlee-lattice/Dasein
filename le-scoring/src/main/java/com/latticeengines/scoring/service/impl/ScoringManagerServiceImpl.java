package com.latticeengines.scoring.service.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.service.ScoringCommandLogService;
import com.latticeengines.scoring.service.ScoringManagerService;
import com.latticeengines.scoring.service.ScoringStepProcessor;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;


@DisallowConcurrentExecution
@Component("scoringManagerService")
public class ScoringManagerServiceImpl extends QuartzJobBean implements ScoringManagerService{

    private static final Log log = LogFactory.getLog(ScoringManagerServiceImpl.class);

    private AsyncTaskExecutor scoringProcessorExecutor;
    
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    private ScoringCommandLogService scoringCommandLogService;
    
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;
    
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    private ScoringStepProcessor scoringStepFinishProcessor;
    
    private ModelingJobService modelingJobService;
    
    private MetadataService metadataService;
    
    private Configuration yarnConfiguration;
    
    private String appTimeLineWebAppAddress;

    private int waitTime = 180;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        log.info("ScoringManager started!");
        log.info("look at database rows:" + scoringCommandEntityMgr.getPopulated());
        ScoringCommand sc = new ScoringCommand(1L, "Nutanix", ScoringCommandStatus.POPULATED, "Q_EventTable_Nutanix", 0, 100,
                new Timestamp(System.currentTimeMillis()));
        if (scoringCommandEntityMgr.findByKey(sc) == null){
        scoringCommandEntityMgr.create(new ScoringCommand(1L, "Nutanix", ScoringCommandStatus.POPULATED, "Q_EventTable_Nutanix", 0, 100,
                new Timestamp(System.currentTimeMillis())));
        }
        
        List<Future<Long>> futures = new ArrayList<>();
        List<ScoringCommand> scoringCommands = scoringCommandEntityMgr.getPopulated();
        for(ScoringCommand scoringCommand : scoringCommands){
            futures.add(scoringProcessorExecutor.submit(new ScoringProcessorCallable(scoringCommand, scoringCommandEntityMgr, scoringCommandLogService, scoringCommandStateEntityMgr, 
                    scoringCommandResultEntityMgr, scoringStepYarnProcessor, scoringStepFinishProcessor,
                    modelingJobService, metadataService, yarnConfiguration, appTimeLineWebAppAddress)));
        }

        for(Future<Long> future : futures){ 
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

    public ScoringCommandLogService getScoringCommandLogService(){
        return this.scoringCommandLogService;
    }
    
    public void setScoringCommandLogService(ScoringCommandLogService scoringCommandLogService) {
        this.scoringCommandLogService = scoringCommandLogService;
    }

    public ScoringCommandStateEntityMgr getScoringCommandStateEntityMgr(){
        return this.scoringCommandStateEntityMgr;
    }

    public void setScoringCommandStateEntityMgr(ScoringCommandStateEntityMgr scoringCommandStateEntityMgr) {
        this.scoringCommandStateEntityMgr = scoringCommandStateEntityMgr;
    }

    public ScoringCommandResultEntityMgr getScoringCommandResultEntityMgr(){
        return this.scoringCommandResultEntityMgr;
    }

    public void setScoringCommandResultEntityMgr(ScoringCommandResultEntityMgr scoringCommandResultEntityMgr) {
        this.scoringCommandResultEntityMgr = scoringCommandResultEntityMgr;
    }

    public ScoringStepYarnProcessor getScoringStepYarnProcessor(){
        return this.scoringStepYarnProcessor;
    }
    
    public void setScoringStepYarnProcessor(ScoringStepYarnProcessor scoringStepYarnProcessor) {
        this.scoringStepYarnProcessor = scoringStepYarnProcessor;
    }
    
    public ScoringStepProcessor getScoringStepFinishProcessor(){
        return this.scoringStepFinishProcessor;
    }

    public void setScoringStepFinishProcessor(ScoringStepProcessor scoringStepFinishProcessor) {
        this.scoringStepFinishProcessor = scoringStepFinishProcessor;
    }
    
    public ModelingJobService getModelingJobService() {
        return modelingJobService;
    }

    public void setModelingJobService(ModelingJobService modelingJobService) {
        this.modelingJobService = modelingJobService;
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public void setMetadataService(MetadataService metadataService) {
        this.metadataService = metadataService;
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
}