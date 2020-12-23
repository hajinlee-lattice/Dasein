package com.latticeengines.app.exposed.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.ActivityAlertsCleanupService;
import com.latticeengines.app.exposed.service.impl.ActivityAlertsCleanupCallable;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("alertCleanupJob")
public class ActivityAlertsCleanupJobBean implements QuartzJobBean {

    private static final Logger log = LoggerFactory.getLogger(ActivityAlertsCleanupJobBean.class);

    @Inject
    private ActivityAlertsCleanupService activityAlertsCleanupService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        log.info(String.format("Got callback with job arguments = %s", jobArguments));

        ActivityAlertsCleanupCallable.Builder builder = new ActivityAlertsCleanupCallable.Builder();
        builder.activityAlertsCleanupService(activityAlertsCleanupService);
        return new ActivityAlertsCleanupCallable(builder);
    }

}
