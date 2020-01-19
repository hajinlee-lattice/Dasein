package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionValidator;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class IngestionValidatorImplTestNG extends DataCloudEtlFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(IngestionValidatorImplTestNG.class);

    @Inject
    private IngestionValidator ingestionValidator;

    @Inject
    private IngestionEntityMgr ingestionEntityMgr;

    private Ingestion ingestion;

    @BeforeClass(groups = "functional")
    public void setup() {
        prepareSimpleIngestion();
    }

    @AfterClass(groups = "functional")
    public void destroy() {
        ingestionEntityMgr.delete(ingestion);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testIsIngestionTriggered() {
        // Case 1: SchedularEnabled is false
        ingestion.setSchedularEnabled(false);
        Assert.assertFalse(ingestionValidator.isIngestionTriggered(ingestion));
        ingestion.setSchedularEnabled(true);
        Assert.assertTrue(ingestionValidator.isIngestionTriggered(ingestion));

        // Case 2: IngestionType PATCH_BOOK should always be manually triggered,
        // no automated trigger
        ingestion.setIngestionType(IngestionType.PATCH_BOOK);
        Assert.assertFalse(ingestionValidator.isIngestionTriggered(ingestion));
        ingestion.setIngestionType(IngestionType.SFTP);
        Assert.assertTrue(ingestionValidator.isIngestionTriggered(ingestion));

        // Case 3:
        Calendar calendar = Calendar.getInstance();
        int intervalInSec = 5;
        ingestion.setCronExpression(
                String.format("*/%d * * ? * %d *", intervalInSec, calendar.get(Calendar.DAY_OF_WEEK)));
        Date latestScheduledTime = CronUtils.getPreviousFireTime(ingestion.getCronExpression()).toDate();
        Date currentTime = new Date();
        log.info("Generated cron expression: {}; Current time: {}, Latest scheduled time: {}",
                ingestion.getCronExpression(), currentTime, latestScheduledTime);
        // No logged trigger time, ingestion should be triggered
        Assert.assertTrue(ingestionValidator.isIngestionTriggered(ingestion));

        // Insert a progress to DB with start time equal to latestScheduledTime,
        // ingestion should not be triggered.
        // Corner case which might fail the test: When isIngestionTriggered() is
        // called again, CronUtils.getPreviousFireTime returns next scheduled
        // time. Add retry for this test to minimize the chance to hit the
        // corner case.
        ingestionEntityMgr.logTriggerTime(Arrays.asList(ingestion), latestScheduledTime);
        Assert.assertFalse(ingestionValidator.isIngestionTriggered(ingestion));
        ingestionEntityMgr.logTriggerTime(Arrays.asList(ingestion), new Date());
        Assert.assertFalse(ingestionValidator.isIngestionTriggered(ingestion));

        // Wait for (intervalInSec + 1) seconds, CronUtils.getPreviousFireTime
        // should return next scheduled time, ingestion should be triggered
        // again
        SleepUtils.sleep(1000 * (intervalInSec + 1));
        Assert.assertTrue(ingestionValidator.isIngestionTriggered(ingestion));
    }

    private void prepareSimpleIngestion() {
        ingestion = new Ingestion();
        ingestion.setIngestionName(IngestionValidatorImplTestNG.class.getSimpleName());
        ingestion.setConfig("");
        ingestion.setSchedularEnabled(false);
        ingestion.setNewJobMaxRetry(3);
        ingestion.setNewJobRetryInterval(10000L);
        ingestion.setIngestionType(IngestionType.SFTP);
        ingestionEntityMgr.save(ingestion);
        // populate PID for later deletion
        ingestion = ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName());
    }
}
