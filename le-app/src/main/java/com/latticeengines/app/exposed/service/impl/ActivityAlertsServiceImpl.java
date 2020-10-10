package com.latticeengines.app.exposed.service.impl;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.app.exposed.entitymanager.ActivityAlertEntityMgr;
import com.latticeengines.app.exposed.service.ActivityAlertsService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component("activityAlertsService")
public class ActivityAlertsServiceImpl implements ActivityAlertsService {
    private static final Logger log = LoggerFactory.getLogger(ActivityAlertsServiceImpl.class);

    @Inject
    private ActivityAlertEntityMgr activityAlertEntityMgr;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private DataLakeService dataLakeService;

    @Override
    public DataPage findActivityAlertsByAccountAndCategory(String customerSpace, String accountId,
            AlertCategory category, int max, Map<String, String> orgInfo) {
        // todo: use tenant fakedate

        String internalAccountId = dataLakeService.getInternalAccountId(accountId, orgInfo);
        if (StringUtils.isBlank(internalAccountId)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "Unable to find any account in Atlas by accountid/lookupid of %s, customerSpace: %s",
                            accountId, customerSpace) });
        }

        String version = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace, null)
                .getActivityAlertVersion();
        log.info(String.format("Found Activity alerts version: %s for tenant: %s", version, customerSpace));
        log.info(String.format("Found Activity alerts version: %s for tenant: %s", version, customerSpace));

        List<ActivityAlert> alerts = activityAlertEntityMgr.findTopNAlertsByEntityId(internalAccountId,
                BusinessEntity.Account, version, category, max);
        if (CollectionUtils.isEmpty(alerts)) {
            log.warn(String.format(
                    "No Alerts found for the Account=%s ( internalId=%s ) Category=%s Version=%s Tenant=%s", accountId,
                    internalAccountId, category.name(), version, customerSpace));
            if (category == AlertCategory.PEOPLE)
                alerts = generateDummyPeopleAlerts(internalAccountId, version, 0L);
            else
                alerts = generateDummyProductsAlerts(internalAccountId, version, 0L);

            // Todo:uncomment later
            // return new DataPage();
        }

        Map<String, ActivityAlertsConfig> alertsNameToAlertConfig = activityStoreProxy
                .getActivityAlertsConfiguration(customerSpace).stream().filter(x -> x.getAlertCategory() == category)
                .collect(Collectors.toMap(ActivityAlertsConfig::getName, Function.identity()));
        if (MapUtils.isEmpty(alertsNameToAlertConfig)) {
            // Shouldn't really ever happen
            log.warn(String.format(
                    "No Alert configuration found, (even though alerts exist!) for the Account=%s ( internalId=%s ) Category=%s Version=%s Tenant=%s Although",
                    accountId, internalAccountId, category.name(), version, customerSpace));
            return new DataPage();
        }

        return buildDataPage(alerts, alertsNameToAlertConfig);
    }

    private DataPage buildDataPage(List<ActivityAlert> alerts,
            Map<String, ActivityAlertsConfig> alertsNameToAlertConfig) {
        DataPage dataPage = new DataPage();
        dataPage.setData(
                alerts.stream().map(x -> new ActivityAlertDTO(x, alertsNameToAlertConfig.get(x.getAlertName()))) //
                        .map(x -> (Map<String, Object>) new ObjectMapper().convertValue(x, Map.class)) //
                        .collect(Collectors.toList()));
        return dataPage;
    }

    private static class ActivityAlertDTO {
        @JsonProperty("Pid")
        private Long pid;

        @JsonProperty("EntityId")
        private String entityId;

        @JsonProperty("EntityType")
        private BusinessEntity entityType;

        @JsonProperty("CreationTimestamp")
        private Long creationTimestamp;

        @JsonProperty("Version")
        private String version;

        @JsonProperty("AlertName")
        private String alertName;

        @JsonProperty("AlertCategory")
        private String category;

        @JsonProperty("AlertHeader")
        private String alertHeader;

        @JsonProperty("QualificationPeriodDays")
        private long qualificationPeriodDays;

        @JsonProperty("AlertMessage")
        private String alertMessage;

        @JsonProperty("IsActive")
        private boolean isActive;

        @JsonProperty("StartTimestamp")
        private Long startTimestamp;

        @JsonProperty("EndTimestamp")
        private Long endTimestamp;

        ActivityAlertDTO(ActivityAlert alert, ActivityAlertsConfig alertsConfig) {
            this.pid = alert.getPid();
            this.alertName = alert.getAlertName();
            this.alertHeader = alertsConfig.getAlertHeader();
            this.alertMessage = TemplateUtils.renderByMap(alertsConfig.getAlertMessageTemplate(), alert.getAlertData());
            this.category = alert.getCategory().getDisplayName();
            this.version = alert.getVersion();
            this.creationTimestamp = alert.getCreationTimestamp().getTime();
            this.entityId = alert.getEntityId();
            this.entityType = alert.getEntityType();
            this.isActive = alertsConfig.isActive();
            this.qualificationPeriodDays = alertsConfig.getQualificationPeriodDays();
            this.startTimestamp = toLong(alert.getAlertData().get(ActivityStoreConstants.Alert.COL_START_TIMESTAMP));
            this.endTimestamp = toLong(alert.getAlertData().get(ActivityStoreConstants.Alert.COL_END_TIMESTAMP));
        }

        private Long toLong(Object ts) {
            return ts instanceof Integer ? Long.valueOf((Integer) ts) : (Long) ts;
        }
    }

    private List<ActivityAlert> generateDummyPeopleAlerts(String accountId, String version, long pid) {
        List<ActivityAlert> alerts = new ArrayList<>();

        // record 1
        ActivityAlert record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(accountId);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(pid);
        record.setCreationTimestamp(new Date());
        record.setVersion(version);
        record.setCategory(AlertCategory.PEOPLE);

        Map<String, Object> data = new HashMap<>();
        Instant end = Instant.now();
        Instant start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        Map<String, Object> alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 2);
        alertData.put("NumResearchIntents", 5);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 2
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(accountId);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(pid);
        record.setCreationTimestamp(Date.from(Instant.now().minus(10, ChronoUnit.HOURS)));
        record.setVersion(version);
        record.setCategory(AlertCategory.PEOPLE);

        data = new HashMap<>();
        end = Instant.now().minus(1, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 2);
        alertData.put("NumResearchIntents", 0);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 3
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(accountId);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(pid);
        record.setCreationTimestamp(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)));
        record.setVersion(version);
        record.setCategory(AlertCategory.PEOPLE);

        data = new HashMap<>();
        end = Instant.now().minus(7, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 0);
        alertData.put("NumResearchIntents", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 4
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.SHOWN_INTENT);
        record.setEntityId(accountId);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(pid);
        record.setCreationTimestamp(Date.from(Instant.now().minus(2, ChronoUnit.DAYS)));
        record.setVersion(version);
        record.setCategory(AlertCategory.PEOPLE);

        data = new HashMap<>();
        end = Instant.now().minus(11, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("NumBuyIntents", 1);
        alertData.put("NumResearchIntents", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        return alerts;
    }

    private List<ActivityAlert> generateDummyProductsAlerts(String accountId, String version, long pid) {
        List<ActivityAlert> alerts = new ArrayList<>();

        // record 1
        ActivityAlert record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY);
        record.setEntityId(accountId);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(pid);
        record.setCreationTimestamp(new Date());
        record.setVersion(version);
        record.setCategory(AlertCategory.PRODUCTS);

        Map<String, Object> data = new HashMap<>();
        Instant end = Instant.now();
        Instant start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        Map<String, Object> alertData = new HashMap<>();
        alertData.put("PageVisits", 392);
        alertData.put("PageName", "Main Page");
        alertData.put("ActiveContacts", 3);
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 2
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.INC_WEB_ACTIVITY_ON_PRODUCT);
        record.setEntityId(accountId);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(pid);
        record.setCreationTimestamp(Date.from(Instant.now().minus(10, ChronoUnit.HOURS)));
        record.setVersion(version);
        record.setCategory(AlertCategory.PRODUCTS);

        data = new HashMap<>();
        end = Instant.now().minus(1, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        alertData.put("PageVisits", 349);
        alertData.put("PageName", "About-Us");
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);

        // record 3
        record = new ActivityAlert();
        record.setAlertName(ActivityStoreConstants.Alert.RE_ENGAGED_ACTIVITY);
        record.setEntityId(accountId);
        record.setEntityType(BusinessEntity.Account);
        record.setTenantId(pid);
        record.setCreationTimestamp(Date.from(Instant.now().minus(1, ChronoUnit.DAYS)));
        record.setVersion(version);
        record.setCategory(AlertCategory.PRODUCTS);

        data = new HashMap<>();
        end = Instant.now().minus(7, ChronoUnit.DAYS);
        start = end.minus(10, ChronoUnit.DAYS);
        data.put(ActivityStoreConstants.Alert.COL_START_TIMESTAMP, start.getEpochSecond());
        data.put(ActivityStoreConstants.Alert.COL_END_TIMESTAMP, end.getEpochSecond());
        alertData = new HashMap<>();
        data.put(ActivityStoreConstants.Alert.COL_ALERT_DATA, alertData);
        record.setAlertData(data);

        alerts.add(record);
        return alerts;
    }

    @VisibleForTesting
    void setDataLakeService(DataLakeService dataLakeService) {
        this.dataLakeService = dataLakeService;
    }
}
