package com.latticeengines.dataplatform.service.impl;

import java.util.Collection;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.JobWatchdogService;
import com.latticeengines.dataplatform.service.impl.watchdog.WatchdogPlugin;

public class JobWatchdogServiceImpl extends QuartzJobBean implements JobWatchdogService {

    private JobService jobService;
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
    private ModelEntityMgr modelEntityMgr;
    private YarnService yarnService;
    private JobEntityMgr jobEntityMgr;
    private int retryWaitTime = 30000;
    private Map<String, WatchdogPlugin> plugins = WatchdogPlugin.getPlugins();

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        run(context);
    }

    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
        for (WatchdogPlugin plugin : plugins.values()) {
            plugin.run(context);
        }
    }

    public JobService getJobService() {
        return jobService;
    }

    public void setJobService(final JobService jobService) {
        this.jobService = jobService;
        new DoForAllPlugins(plugins.values()) {

            @Override
            public void execute(WatchdogPlugin plugin) {
                plugin.setJobService(jobService);
            }
            
        }.execute();
    }

    public ThrottleConfigurationEntityMgr getThrottleConfigurationEntityMgr() {
        return throttleConfigurationEntityMgr;
    }

    public void setThrottleConfigurationEntityMgr(final ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr) {
        this.throttleConfigurationEntityMgr = throttleConfigurationEntityMgr;
        new DoForAllPlugins(plugins.values()) {

            @Override
            public void execute(WatchdogPlugin plugin) {
                plugin.setThrottleConfigurationEntityMgr(throttleConfigurationEntityMgr);
            }
            
        }.execute();
    }

    public ModelEntityMgr getModelEntityMgr() {
        return modelEntityMgr;
    }

    public void setModelEntityMgr(final ModelEntityMgr modelEntityMgr) {
        this.modelEntityMgr = modelEntityMgr;
        new DoForAllPlugins(plugins.values()) {

            @Override
            public void execute(WatchdogPlugin plugin) {
                plugin.setModelEntityMgr(modelEntityMgr);
            }
            
        }.execute();
    }

    public YarnService getYarnService() {
        return yarnService;
    }

    public void setYarnService(final YarnService yarnService) {
        this.yarnService = yarnService;
        new DoForAllPlugins(plugins.values()) {

            @Override
            public void execute(WatchdogPlugin plugin) {
                plugin.setYarnService(yarnService);
            }
            
        }.execute();
    }

    public JobEntityMgr getJobEntityMgr() {
        return jobEntityMgr;
    }

    public void setJobEntityMgr(final JobEntityMgr jobEntityMgr) {
        this.jobEntityMgr = jobEntityMgr;
        new DoForAllPlugins(plugins.values()) {

            @Override
            public void execute(WatchdogPlugin plugin) {
                plugin.setJobEntityMgr(jobEntityMgr);
            }
            
        }.execute();
    }

    public int getRetryWaitTime() {
        return retryWaitTime;
    }

    public void setRetryWaitTime(final int retryWaitTime) {
        this.retryWaitTime = retryWaitTime;
        new DoForAllPlugins(plugins.values()) {

            @Override
            public void execute(WatchdogPlugin plugin) {
                plugin.setRetryWaitTime(retryWaitTime);
            }
            
        }.execute();
    }

    private abstract class DoForAllPlugins {
        private Collection<WatchdogPlugin> plugins;
        
        DoForAllPlugins(Collection<WatchdogPlugin> plugins) {
            this.plugins = plugins;
        }
        
        public void execute() {
            for (WatchdogPlugin plugin : plugins) {
                execute(plugin);
            }
        }
        
        public abstract void execute(WatchdogPlugin plugin);
    }
}
