package com.latticeengines.spark.exposed.job.cdl;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
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

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    private static final Logger log = LoggerFactory.getLogger(PublishActivityAlertsJobTestNG.class);

    @Test(groups = "functional")
    public void testPublishActivityAlerts() {
        PublishActivityAlertsJobConfig config = new PublishActivityAlertsJobConfig();
        config.setTableToPublish(HdfsDataUnit.fromPath("/tmp/jlmehta/GenerateActivityAlertJobTestNG/UeGWbH/Output2"));
        config.setDbDriver(dataDbDriver);
        config.setDbUrl(dataDbUrl);
        config.setDbUser(dataDbUser);
        config.setDbPassword(CipherUtils.encrypt(dataDbPassword));
        config.setDbTableName(ActivityAlert.TABLE_NAME);
        config.setAlertVersion("version");
        config.setTenantId(10L);
        Map<String, AlertCategory> map = new HashMap<>();
        map.put(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY, AlertCategory.PRODUCTS);
        map.put(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY_ON_PRODUCT, AlertCategory.PRODUCTS);
        map.put(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY, AlertCategory.PRODUCTS);
        map.put(ActivityStoreConstants.Alert.SHOWN_INTENT, AlertCategory.PEOPLE);
        config.setAlertNameToAlertCategory(map);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(PublishActivityAlertsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 0);
    }
}
