package com.latticeengines.serviceflows.workflow.export;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.ENABLE_ACCOUNT360;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PublishActivityAlertsJobConfig;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.PublishActivityAlertsJob;

@Component("publishActivityAlerts")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishActivityAlerts extends RunSparkJob<TimeLineSparkStepConfiguration, PublishActivityAlertsJobConfig> {
    private static final Logger log = LoggerFactory.getLogger(PublishActivityAlerts.class);

    @Inject
    private BatonService batonService;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    @Override
    protected Class<? extends AbstractSparkJob<PublishActivityAlertsJobConfig>> getJobClz() {
        return PublishActivityAlertsJob.class;
    }

    @Override
    protected PublishActivityAlertsJobConfig configureJob(TimeLineSparkStepConfiguration stepConfiguration) {
        if (!account360Enabled()) {
            log.info("Skip publishing Activity alerts table. Account360 enabled = {}", account360Enabled());
            return null;
        }
        if (BooleanUtils.isTrue(getObjectFromContext(ACTIVITY_ALERT_PUBLISHED, Boolean.class))) {
            log.info("In short-cut mode, skip publishing Activity alerts table");
            return null;
        }

        Boolean wereAlertsGenerated = getObjectFromContext(ACTIVITY_ALERT_GENERATED, Boolean.class);
        if (wereAlertsGenerated == null || wereAlertsGenerated == Boolean.FALSE) {
            log.info("No alerts were generated, skip publishing alerts");
            return null;
        }
        String alertTableName;
        if (configuration.isShouldRebuild()) {
            alertTableName = getStringValueFromContext(ACTIVITY_ALERT_MASTER_TABLE_NAME);
            log.info("In rebuild mode, publishing master activity alert data, tablename {}", alertTableName);
        } else {
            alertTableName = getStringValueFromContext(ACTIVITY_ALERT_DIFF_TABLE_NAME);
            log.info("Publishing diff activity alert data, tablename {}", alertTableName);
        }

        if (StringUtils.isBlank(alertTableName)) {
            log.warn("No alerts diff table found");
            return null;
        }

        Tenant tenant = tenantEntityMgr.findByTenantId(stepConfiguration.getCustomer());

        List<ActivityAlertsConfig> alertConfigs = getAlertConfigs();

        Table alertTable = metadataProxy.getTable(stepConfiguration.getCustomer(), alertTableName);
        String key = CipherUtils.generateKey();
        String random = RandomStringUtils.randomAlphanumeric(24);
        PublishActivityAlertsJobConfig config = new PublishActivityAlertsJobConfig();
        DataUnit alertTableDU = toDataUnit(alertTable, "AlertTable");
        config.setInput(Collections.singletonList(alertTableDU));
        config.setDbDriver(dataDbDriver);
        config.setDbUrl(dataDbUrl);
        config.setDbUser(dataDbUser);
        config.setDbPassword(CipherUtils.encrypt(dataDbPassword, key, random));
        config.setDbRandomStr(random + key);
        config.setDbTableName(ActivityAlert.TABLE_NAME);
        config.setAlertVersion(getActivityAlertVersion());
        config.setTenantId(tenant.getPid());
        config.alertNameToAlertCategory.putAll(alertConfigs.stream() //
                .filter(Objects::nonNull) //
                .filter(ActivityAlertsConfig::isActive) //
                .map(alertConfig -> Pair.of(alertConfig.getName(), alertConfig.getAlertCategory().name())) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1)));
        return config;
    }

    private String getActivityAlertVersion() {
        // Alert version creation/reset is handled in GenerateActivityAlertsStep, so
        // just get the version from context
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        String version = dcStatus.getActivityAlertVersion();
        if (configuration.isShouldRebuild()) {
            log.info("In rebuild mode, activity alert version {}", version);
        } else {
            log.info("Activity alert version {}", version);
        }
        return version;
    }

    private List<ActivityAlertsConfig> getAlertConfigs() {
        List<ActivityAlertsConfig> configs = ListUtils
                .emptyIfNull(activityStoreProxy.getActivityAlertsConfiguration(configuration.getCustomer()));
        log.info("ActivityConfigs = {} for tenant {}", JsonUtils.serialize(configs), configuration.getCustomer());
        return configs;
    }

    private boolean account360Enabled() {
        return batonService.isEnabled(CustomerSpace.parse(configuration.getCustomer()), ENABLE_ACCOUNT360);
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        log.info("Activity alert publication completed");
        putObjectInContext(ACTIVITY_ALERT_PUBLISHED, true);
    }

    @Override
    public boolean skipOnMissingConfiguration() {
        // for retrying old jobs from previous release
        return true;
    }
}
