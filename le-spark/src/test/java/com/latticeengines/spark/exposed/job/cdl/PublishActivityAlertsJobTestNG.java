package com.latticeengines.spark.exposed.job.cdl;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    String inputPath;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;
    private static final Logger log = LoggerFactory.getLogger(PublishActivityAlertsJobTestNG.class);

    @BeforeClass
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
    }

    @Test(groups = "functional", enabled = false)
    public void testPublishActivityAlerts() {
        PublishActivityAlertsJobConfig config = new PublishActivityAlertsJobConfig();
        config.setInput(Collections.singletonList(HdfsDataUnit.fromPath(inputPath)));
        config.setDbDriver(dataDbDriver);
        config.setDbUrl(dataDbUrl);
        config.setDbUser(dataDbUser);
        config.setDbPassword(CipherUtils.encrypt(dataDbPassword));
        config.setDbTableName(ActivityAlert.TABLE_NAME);
        config.setAlertVersion("version");
        config.setTenantId(10L);
        Map<String, String> map = new HashMap<>();
        map.put(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY, AlertCategory.PRODUCTS.name());
        map.put(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY_ON_PRODUCT, AlertCategory.PRODUCTS.name());
        map.put(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY, AlertCategory.PRODUCTS.name());
        map.put(ActivityStoreConstants.Alert.SHOWN_INTENT, AlertCategory.PEOPLE.name());
        config.setAlertNameToAlertCategory(map);
        inputProvider = config::getInput;

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(PublishActivityAlertsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 0);
    }
}
