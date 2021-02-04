package com.latticeengines.app.exposed.service.impl;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.entitymanager.ActivityAlertEntityMgr;
import com.latticeengines.app.exposed.service.ActivityAlertsCleanupService;

@Component("activityAlertsCleanupService")
public class ActivityAlertsCleanupServiceImpl implements ActivityAlertsCleanupService {

    private static final Logger log = LoggerFactory.getLogger(ActivityAlertsCleanupServiceImpl.class);

    @Inject
    private ActivityAlertEntityMgr activityAlertEntityMgr;

    @Value("${app.activity.alert.days.keep:90}")
    private Long DAYS_TO_KEEP_ACTIVITY_ALERT;

    @Value("${app.activity.alert.update.max:1000}")
    private int maxUpdateRows;

    @Override
    public void cleanup() {
        cleanupActivityAlertDueToExpiration();
    }

    private void cleanupActivityAlertDueToExpiration() {
        Date expireDate = Date.from(Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS)
                .minusDays(DAYS_TO_KEEP_ACTIVITY_ALERT).toInstant());
        boolean shouldLoop = true;
        int deletedCount = 0;
        log.info("Cleaning up starts for activity alerts expired");
        try {
            while (shouldLoop) {
                int updatedCount = activityAlertEntityMgr.deleteByExpireDateBefore(expireDate, maxUpdateRows);
                shouldLoop = updatedCount > 0;
                deletedCount += updatedCount;
                if (shouldLoop) {
                    log.info(String.format(
                            "Deleting Activity Alerts expired: expireDate = %tF, maxUpdateRows = %d, deletedCount = %d",
                            expireDate, maxUpdateRows, updatedCount));
                }
            }
            if (deletedCount > 0) {
                log.info(String.format("cleanupActivityAlerts: Completed cleanup activity alerts " + "(count = %d)",
                        deletedCount));
            }

        } catch (Exception ex) {
            log.error(String.format("Failed to cleanup expired activity alters before %tF", expireDate), ex);
        }
    }
}
