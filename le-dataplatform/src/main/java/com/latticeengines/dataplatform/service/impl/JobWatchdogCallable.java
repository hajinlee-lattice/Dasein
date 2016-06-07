package com.latticeengines.dataplatform.service.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.impl.watchdog.WatchdogPlugin;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;

public class JobWatchdogCallable implements Callable<Boolean> {

    private Map<String, WatchdogPlugin> plugins;

    public JobWatchdogCallable(Builder builder) {
        this.plugins = builder.getPlugins();
    }

    @Override
    public Boolean call() throws Exception {
        for (WatchdogPlugin plugin : plugins.values()) {
            plugin.run(null);
        }
        return true;
    }

    public static class Builder {

        private ModelingJobService modelingJobService;
        private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
        private ModelEntityMgr modelEntityMgr;
        private YarnService yarnService;
        private JobEntityMgr jobEntityMgr;
        private int retryWaitTime = 30000;
        private Map<String, WatchdogPlugin> plugins = WatchdogPlugin.getPlugins();

        public Builder() {

        }

        public ModelingJobService getModelingJobService() {
            return modelingJobService;
        }

        public ThrottleConfigurationEntityMgr getThrottleConfigurationEntityMgr() {
            return throttleConfigurationEntityMgr;
        }

        public ModelEntityMgr getModelEntityMgr() {
            return modelEntityMgr;
        }

        public YarnService getYarnService() {
            return yarnService;
        }

        public JobEntityMgr getJobEntityMgr() {
            return jobEntityMgr;
        }

        public int getRetryWaitTime() {
            return retryWaitTime;
        }

        public Map<String, WatchdogPlugin> getPlugins() {
            return plugins;
        }

        public Builder modelingJobService(final ModelingJobService modelingJobService) {
            this.modelingJobService = modelingJobService;
            new DoForAllPlugins(plugins.values()) {

                @Override
                public void execute(WatchdogPlugin plugin) {
                    plugin.setModelingJobService(modelingJobService);
                }

            }.execute();
            return this;
        }

        public Builder throttleConfigurationEntityMgr(
                final ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr) {
            this.throttleConfigurationEntityMgr = throttleConfigurationEntityMgr;
            new DoForAllPlugins(plugins.values()) {

                @Override
                public void execute(WatchdogPlugin plugin) {
                    plugin.setThrottleConfigurationEntityMgr(throttleConfigurationEntityMgr);
                }

            }.execute();
            return this;
        }

        public Builder modelEntityMgr(final ModelEntityMgr modelEntityMgr) {
            this.modelEntityMgr = modelEntityMgr;
            new DoForAllPlugins(plugins.values()) {

                @Override
                public void execute(WatchdogPlugin plugin) {
                    plugin.setModelEntityMgr(modelEntityMgr);
                }

            }.execute();
            return this;
        }

        public Builder yarnService(final YarnService yarnService) {
            this.yarnService = yarnService;
            new DoForAllPlugins(plugins.values()) {

                @Override
                public void execute(WatchdogPlugin plugin) {
                    plugin.setYarnService(yarnService);
                }

            }.execute();
            return this;
        }

        public Builder jobEntityMgr(final JobEntityMgr jobEntityMgr) {
            this.jobEntityMgr = jobEntityMgr;
            new DoForAllPlugins(plugins.values()) {

                @Override
                public void execute(WatchdogPlugin plugin) {
                    plugin.setJobEntityMgr(jobEntityMgr);
                }

            }.execute();
            return this;
        }

        public Builder retryWaitTime(final int retryWaitTime) {
            this.retryWaitTime = retryWaitTime;
            new DoForAllPlugins(plugins.values()) {

                @Override
                public void execute(WatchdogPlugin plugin) {
                    plugin.setRetryWaitTime(retryWaitTime);
                }

            }.execute();
            return this;
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

}
