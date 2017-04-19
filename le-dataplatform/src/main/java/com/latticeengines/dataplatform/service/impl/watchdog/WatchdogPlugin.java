package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;

public abstract class WatchdogPlugin {

    protected ModelingJobService modelingJobService;
    protected ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
    protected ModelEntityMgr modelEntityMgr;
    protected YarnService yarnService;
    protected JobEntityMgr jobEntityMgr;
    protected int retryWaitTime = 80000;
    protected int maxRetryTimeThreshold = 5400000;

    private static Map<String, WatchdogPlugin> plugins = new HashMap<String, WatchdogPlugin>();

    protected static void register(WatchdogPlugin plugin) {
        plugins.put(plugin.getName(), plugin);
    }

    public static Map<String, WatchdogPlugin> getPlugins() {
        return plugins;
    }

    public abstract void run();

    public String getName() {
        String fqn = getClass().getName();
        return fqn.substring(fqn.lastIndexOf(".") + 1);
    }

    public ModelingJobService getModelingJobService() {
        return modelingJobService;
    }

    public void setModelingJobService(final ModelingJobService modelingJobService) {
        this.modelingJobService = modelingJobService;
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
