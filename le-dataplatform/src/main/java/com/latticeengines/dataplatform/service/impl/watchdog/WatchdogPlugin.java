package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.HashMap;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.JobService;

public abstract class WatchdogPlugin {

    protected JobService jobService;
    protected ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
    protected ModelEntityMgr modelEntityMgr;
    protected YarnService yarnService;
    protected JobEntityMgr jobEntityMgr;
    protected int retryWaitTime = 60000;
    
    private static Map<String, WatchdogPlugin> plugins = new HashMap<String, WatchdogPlugin>();
    
    protected static void register(WatchdogPlugin plugin) {
        plugins.put(plugin.getName(), plugin);
    }
    
    public static Map<String, WatchdogPlugin> getPlugins() {
        return plugins;
    }
    
    public abstract void run(JobExecutionContext context) throws JobExecutionException;
    
    public String getName() {
        String fqn = getClass().getName();
        return fqn.substring(fqn.lastIndexOf(".") + 1);
    }
    
    public JobService getJobService() {
        return jobService;
    }

    public void setJobService(final JobService jobService) {
        this.jobService = jobService;
    }

    public ThrottleConfigurationEntityMgr getThrottleConfigurationEntityMgr() {
        return throttleConfigurationEntityMgr;
    }

    public void setThrottleConfigurationEntityMgr(final ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr) {
        this.throttleConfigurationEntityMgr = throttleConfigurationEntityMgr;
    }

    public ModelEntityMgr getModelEntityMgr() {
        return modelEntityMgr;
    }

    public void setModelEntityMgr(final ModelEntityMgr modelEntityMgr) {
        this.modelEntityMgr = modelEntityMgr;
    }

    public YarnService getYarnService() {
        return yarnService;
    }

    public void setYarnService(final YarnService yarnService) {
        this.yarnService = yarnService;
    }

    public JobEntityMgr getJobEntityMgr() {
        return jobEntityMgr;
    }

    public void setJobEntityMgr(final JobEntityMgr jobEntityMgr) {
        this.jobEntityMgr = jobEntityMgr;
    }

    public int getRetryWaitTime() {
        return retryWaitTime;
    }

    public void setRetryWaitTime(final int retryWaitTime) {
        this.retryWaitTime = retryWaitTime;
    }

}
