package com.latticeengines.spark.exposed.job.cdl;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PublishActivityAlertsJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class PublishActivityAlertsJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final String ALERT_VERSION = "version";

    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    private static final Logger log = LoggerFactory.getLogger(PublishActivityAlertsJobTestNG.class);
    private String inputPath;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        String fileName = "publish-alerts-input.avro";
        String testInput = "activity/alerts/" + fileName;
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL resourceAsStream = classLoader.getResource(testInput);
        Assert.assertNotNull(resourceAsStream);
        inputPath = getWorkspace();
        try {
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, resourceAsStream.getPath(), inputPath + "/" + fileName);
        } catch (Exception e) {
            Assert.fail("Failed in setup\n" + e.getMessage());
        }

        try {
            Connection conn = DriverManager.getConnection(dataDbUrl, dataDbUser, dataDbPassword);
            Statement stmt = conn.createStatement();
            stmt.execute(
                    "DELETE FROM Data_MultiTenant.ActivityAlert WHERE TENANT_ID = -1 AND VERSION = " + ALERT_VERSION);
        } catch (Exception e) {
            log.warn("Failed to clean up possible existing data");
        }
    }

    @Test(groups = "functional")
    public void testPublishActivityAlerts() throws SQLException {
        String key = CipherUtils.generateKey();
        String random = RandomStringUtils.randomAlphanumeric(24);
        PublishActivityAlertsJobConfig config = new PublishActivityAlertsJobConfig();
        config.setInput(Collections.singletonList(HdfsDataUnit.fromPath(inputPath)));
        config.setDbDriver(dataDbDriver);
        config.setDbUrl(dataDbUrl);
        config.setDbUser(dataDbUser);
        config.setDbPassword(CipherUtils.encrypt(dataDbPassword, key, random));
        config.setDbRandomStr(random + key);
        config.setDbTableName(ActivityAlert.TABLE_NAME);
        config.setAlertVersion(ALERT_VERSION);
        config.setTenantId(-1L);
        Map<String, String> map = new HashMap<>();
        map.put(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY, AlertCategory.PEOPLE.name());
        map.put(ActivityStoreConstants.Alert.ANONYMOUS_WEB_VISITS, AlertCategory.PEOPLE.name());
        map.put(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY, AlertCategory.PEOPLE.name());
        config.setAlertNameToAlertCategory(map);
        inputProvider = config::getInput;

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(PublishActivityAlertsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 0);
        verifyResult();
    }

    private void verifyResult() {
        try (Connection conn = DriverManager.getConnection(dataDbUrl, dataDbUser, dataDbPassword);) {
            Statement stmt = conn.createStatement();
            ResultSet rs;
            int size = 0;
            Map<String, Integer> alertTypeCounts = new HashMap<>();
            rs = stmt.executeQuery("SELECT * FROM Data_MultiTenant.ActivityAlert WHERE TENANT_ID = -1");
            Assert.assertNotNull(rs);
            while (rs.next()) {
                size++;
                String alertName = rs.getString("ALERT_NAME");
                if (!alertTypeCounts.containsKey(alertName))
                    alertTypeCounts.put(alertName, 0);
                alertTypeCounts.put(alertName, alertTypeCounts.get(alertName) + 1);
            }
            Assert.assertEquals(size, 10);
            Assert.assertEquals(alertTypeCounts.get(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY).intValue(), 5);
            Assert.assertEquals(alertTypeCounts.get(ActivityStoreConstants.Alert.ANONYMOUS_WEB_VISITS).intValue(), 4);
            Assert.assertEquals(alertTypeCounts.get(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY).intValue(), 1);
        } catch (Exception e) {
            Assert.fail("Failed to verify\n" + e.getMessage());
        }
    }

}
