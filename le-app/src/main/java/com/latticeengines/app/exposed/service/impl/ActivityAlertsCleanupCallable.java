package com.latticeengines.app.exposed.service.impl;

import java.util.concurrent.Callable;

import com.latticeengines.app.exposed.service.ActivityAlertsCleanupService;

public class ActivityAlertsCleanupCallable implements Callable<Boolean> {

    private ActivityAlertsCleanupService activityAlertsCleanupService;

    public ActivityAlertsCleanupCallable(Builder builder) {
        this.activityAlertsCleanupService = builder.getActivityAlertsCleanupService();
    }

    @Override
    public Boolean call() throws Exception {
        activityAlertsCleanupService.cleanup();
        return true;
    }

    public static class Builder {
        private ActivityAlertsCleanupService activityAlertsCleanupService;

        public Builder() {
        }

        public ActivityAlertsCleanupService getActivityAlertsCleanupService() {
            return activityAlertsCleanupService;
        }

        public Builder activityAlertsCleanupService(ActivityAlertsCleanupService activityAlertsCleanupService) {
            this.activityAlertsCleanupService = activityAlertsCleanupService;
            return this;
        }
    }

}
