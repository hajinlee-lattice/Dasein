package com.latticeengines.modelquality.controller;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.monitor.SlackSettings;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
import com.latticeengines.monitor.exposed.service.SlackService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class AccountMasterModelRunResourceByLocationDeploymentTestNG extends BaseAccountMasterModelRunDeploymentTestNG {

    private static final Logger log = LoggerFactory
            .getLogger(AccountMasterModelRunResourceByLocationDeploymentTestNG.class);

    private static final ImmutableMap<String, String> testCases = ImmutableMap.<String, String>builder() //
            .put("Mulesoft_NA_loc_AccountMaster", "Mulesoft_NA_loc.csv") //
            .put("Mulesoft_Emea_loc_AccountMaster", "Mulesoft_Emea_loc.csv") //
            .put("Mulesoft_Apac_loc_AccountMaster", "Mulesoft_apac_loc.csv") //
            .put("Qlik_loc_AccountMaster", "Qlik_loc.csv") //
            .put("HootSuite_loc_AccountMaster", "HootSuite_loc.csv") //
            .put("CornerStone_loc_AccountMaster", "Corner_loc.csv") //
            .put("PolyCom_loc_AccountMaster", "PolyCom_loc.csv") //
            .put("Tenable_loc_AccountMaster", "Tenable_loc.csv") //
            .build();

    @Inject
    private SlackService slackService;

    @Inject
    private WorkflowProxy workflowProxy;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${modelquality.test.slack.webhook.url:}")
    private String webhookUrl;

    @SuppressWarnings("deprecation")
    @Override
    @BeforeClass(groups = { "deployment", "am", "am_all" })
    public void setup() throws Exception {
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-AccountMaster");
        namedModelRunEntityNames.add("ModelQualityDeploymentTest-DerivedColumn");

        namedAnalyticPipelineEntityNames.add("AccountMasterModelRunResourceDeploymentTestNG");
        namedAnalyticPipelineEntityNames.add("DerivedColumnModelRunResourceDeploymentTestNG");

        deleteLocalEntities();
        super.setup();

        Map<String, Boolean> featureFlagMap = new HashMap<String, Boolean>();
        featureFlagMap.put(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName(), true);
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName(), true);
        featureFlagMap.put(LatticeFeatureFlag.BYPASS_DNB_CACHE.getName(), false);

        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, null, featureFlagMap);
    }

    @Override
    @AfterClass(groups = { "deployment", "am", "am_all" })
    public void tearDown() throws Exception {
        deleteLocalEntities();
        super.tearDown();
    }

    @Test(groups = "am")
    public void runModelForCsv() {
        String dataSetName = getSystemProperty("MQ_DATASET");
        if (StringUtils.isBlank(dataSetName)) {
            Assert.fail(String.format("No dataset specified. MQ_DATASET=%s", dataSetName));
        }
        boolean failFast = Boolean.TRUE.toString().equalsIgnoreCase(getSystemProperty("FAIL_FAST"));
        String[] dataSets = dataSetName.split(",");
        log.info("Data sets = {}", Arrays.toString(dataSets));
        boolean hasFailure = false;
        for (String dataSet : dataSets) {
            if (testCases.containsKey(dataSetName)) {
                String csvFile = testCases.get(dataSetName);
                hasFailure = hasFailure || !runModelWrapper(dataSetName, csvFile, failFast);
            } else {
                log.warn("Skipping run model, dataSetName={}", dataSet);
            }
        }
        Assert.assertFalse(hasFailure);
    }

    @Test(groups = { "am_all" }, dataProvider = "getAccountMasterLocationCsvFile")
    public void runModelAccountMasterLocation(String dataSetName, String csvFile) {
        runModelWrapper(dataSetName, csvFile, true);
    }

    private boolean runModelWrapper(String dataSetName, String csvFile, boolean failFast) {
        if (StringUtils.isBlank(dataSetName) || StringUtils.isBlank(csvFile)) {
            return true;
        }

        Stopwatch timer = Stopwatch.createStarted();
        try {
            log.info("Start model quality test (DataSet={}, CsvFile={})", dataSetName, csvFile);
            notifyModelQualityTestStarted(dataSetName);
            runModelAccountMaster(dataSetName, csvFile);
            log.info("Model quality test (DataSet={}, CsvFile={}) finished successfully", dataSetName, csvFile);
            sendModelQualityTestResult(dataSetName, timer.elapsed(TimeUnit.MILLISECONDS), Status.COMPLETED, null);
            return true;
        } catch (Exception e) {
            log.error(String.format("Fail to run model quality test. DataSet=%s", dataSetName), e);
            sendModelQualityTestResult(dataSetName, timer.elapsed(TimeUnit.MILLISECONDS), Status.FAILED,
                    String.format("Error: %s", e.getMessage()));
            if (failFast) {
                log.error("Failing fast after one test failure");
                throw e;
            }
            return false;
        }
    }

    /*
     * Find the log link for last job in test tenant (workaround since it is hard to
     * extract application id from model run)
     */
    private String getLogLinkForLastJob() {
        try {
            List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(mainTestTenant);
            Optional<Job> lastJob = jobs.stream() //
                    .filter(Objects::nonNull) //
                    .filter(job -> job.getPid() != null) //
                    .max(Comparator.comparing(Job::getPid));
            return lastJob.map(job -> workflowProxy.getLogLinkByWorkflowPid(job.getPid())) //
                    .map(WorkflowLogLinks::getAppMasterUrl) //
                    .orElse(null);
        } catch (Exception e) {
            return null;
        }
    }

    @DataProvider(name = "getAccountMasterLocationCsvFile")
    public Object[][] getAccountMasterLocationCsvFile() {
        ImmutableList<Map.Entry<String, String>> list = testCases.entrySet().asList();
        Object[][] data = new Object[list.size()][2];
        for (int i = 0; i < list.size(); i++) {
            Map.Entry<String, String> entry = list.get(i);
            data[i] = new Object[] { entry.getKey(), entry.getValue() };
        }
        return data;
    }

    private void notifyModelQualityTestStarted(@NotNull String dataSetName) {
        if (StringUtils.isBlank(webhookUrl)) {
            return;
        }
        String dataCloudVersion = getDataCloudVersion();
        String preText = String.format("[%s-%s][DataCloud %s]", leEnv, leStack, dataCloudVersion);
        String message = String.format("Started test for dataset %s.\n Job url: %s", dataSetName,
                getSystemProperty("BUILD_URL"));
        SlackSettings settings = new SlackSettings(webhookUrl, null, preText, message, "ModelQualityTestRunner",
                SlackSettings.Color.NORMAL);
        slackService.sendSlack(settings);
    }

    private void sendModelQualityTestResult(@NotNull String dataSetName, long durationInMillis, @NotNull Status status,
            String extraMessage) {
        if (StringUtils.isBlank(webhookUrl)) {
            return;
        }
        String dataCloudVersion = getDataCloudVersion();
        SlackSettings.Color color = status == Status.FAILED ? SlackSettings.Color.DANGER : SlackSettings.Color.NORMAL;
        String preText = String.format("[%s-%s][DataCloud %s]", leEnv, leStack, dataCloudVersion);
        String title = String.format("Model quality test for dataset %s", dataSetName);
        String message = String.format("Test %s after %s\nLog Link: %s", //
                status.name(), DurationFormatUtils.formatDurationWords(durationInMillis, true, true), //
                getLogLinkForLastJob());
        if (StringUtils.isNotBlank(extraMessage)) {
            message += "\n" + extraMessage;
        }
        SlackSettings settings = new SlackSettings(webhookUrl, title, preText, message, "ModelQualityTestRunner",
                color);
        slackService.sendSlack(settings);
    }

    private enum Status {
        COMPLETED, FAILED
    }
}
