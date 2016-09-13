package com.latticeengines.dataplatform.service.impl;

import java.util.Collection;
import java.util.Map;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.JobWatchdogService;
import com.latticeengines.dataplatform.service.impl.watchdog.WatchdogPlugin;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;

@DisallowConcurrentExecution
public class JobWatchdogServiceImpl implements JobWatchdogService {

    private ModelingJobService modelingJobService;
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
    private ModelEntityMgr modelEntityMgr;
    private YarnService yarnService;
    private JobEntityMgr jobEntityMgr;
    private int retryWaitTime = 30000;
    private Map<String, WatchdogPlugin> plugins = WatchdogPlugin.getPlugins();

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void run(JobExecutionContext context) throws JobExecutionException {
        for (WatchdogPlugin plugin : plugins.values()) {
            plugin.run(context);
        }
    }

    public ModelingJobService getModelingJobService() {
        return modelingJobService;
    }

    public void setModelingJobService(final ModelingJobService modelingJobService) {
        this.modelingJobService = modelingJobService;
        new DoForAllPlugins(plugins.values()) {

            @Override
            public void execute(WatchdogPlugin plugin) {
                plugin.setModelingJobService(modelingJobService);
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
