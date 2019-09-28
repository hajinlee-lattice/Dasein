package com.latticeengines.yarn.functionalframework;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.StringUtils;
import org.springframework.yarn.client.YarnClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.aws.AwsApplicationId;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.yarn.exposed.service.AwsBatchJobService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-yarn-context.xml" })
public class YarnFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(YarnFunctionalTestNGBase.class);

    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 25;

    @Autowired
    protected Configuration yarnConfiguration;

    @Resource(name = "awsBatchjobService")
    protected AwsBatchJobService awsBatchJobService;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

    protected YarnClient yarnClient;

    public YarnFunctionalTestNGBase() {
    }

    public YarnFunctionalTestNGBase(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    @BeforeMethod(enabled = true, firstTimeOnly = true, alwaysRun = true)
    public void beforeEachTest() {
        yarnClient = getYarnClient("defaultYarnClient");
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
    }

    public void setYarnClient(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    public void setAwsBatchJobService(AwsBatchJobService awsBatchJobService) {
        this.awsBatchJobService = awsBatchJobService;
    }

    private YarnClient getYarnClient(String yarnClientName) {
        try {
            if (StringUtils.isEmpty(yarnClientName)) {
                throw new IllegalStateException("Yarn client name cannot be empty.");
            }
            return (YarnClient) applicationContext.getBean(yarnClientName);
        } catch (Throwable e) {
            log.error("Error while getting yarnClient for application " + yarnClientName, e);
        }
        return null;
    }

    public FinalApplicationStatus waitForStatus(ApplicationId applicationId,
            FinalApplicationStatus... applicationStatuses) throws Exception {
        return waitForStatus(applicationId.toString(), MAX_MILLIS_TO_WAIT, applicationStatuses);
    }

    public FinalApplicationStatus waitForStatus(String applicationId, FinalApplicationStatus... applicationStatuses)
            throws Exception {
        return waitForStatus(applicationId, MAX_MILLIS_TO_WAIT, applicationStatuses);
    }

    public FinalApplicationStatus waitForStatus(String applicationId, Long waitTimeInMillis,
            FinalApplicationStatus... applicationStatuses) throws Exception {
        Assert.assertNotNull(applicationId, "ApplicationId must not be null");
        waitTimeInMillis = waitTimeInMillis == null ? MAX_MILLIS_TO_WAIT : waitTimeInMillis;
        log.info(String.format("Waiting on %s for at most %dms.", applicationId, waitTimeInMillis));

        boolean isAwsBatchJob = AwsApplicationId.isAwsBatchJob(applicationId);

        if (!isAwsBatchJob) {
            Assert.assertNotNull(yarnClient, "Yarn client must be set");
        }

        FinalApplicationStatus status;
        long start = System.currentTimeMillis();

        // break label for inner loop
        done: do {
            if (isAwsBatchJob) {
                status = getAwsBatchJobStatus(applicationId);
            } else {
                status = findStatus(yarnClient, applicationId);
            }
            if (status == null) {
                break;
            }
            for (FinalApplicationStatus statusCheck : applicationStatuses) {
                if (status.equals(statusCheck) || YarnUtils.TERMINAL_STATUS.contains(status)) {
                    break done;
                }
            }
            Thread.sleep(5000);
        } while (System.currentTimeMillis() - start < waitTimeInMillis);
        return status;
    }

    private FinalApplicationStatus getAwsBatchJobStatus(String applicationId) {
        JobStatus awsJobStatus = awsBatchJobService.getAwsBatchJobStatus(applicationId);
        if (awsJobStatus != null) {
            return awsJobStatus.getStatus();
        } else {
            return null;
        }
    }

    protected FinalApplicationStatus findStatus(YarnClient client, String applicationId) {
        FinalApplicationStatus status = null;
        for (ApplicationReport report : client.listApplications()) {
            if (report.getApplicationId().toString().equals(applicationId)) {
                status = report.getFinalApplicationStatus();
                break;
            }
        }
        return status;
    }

    public ApplicationId getApplicationId(String appIdStr) {
        if (AwsApplicationId.isAwsBatchJob(appIdStr)) {
            return AwsApplicationId.fromString(appIdStr);
        } else {
            return ApplicationIdUtils.toApplicationIdObj(appIdStr);
        }
    }
}
