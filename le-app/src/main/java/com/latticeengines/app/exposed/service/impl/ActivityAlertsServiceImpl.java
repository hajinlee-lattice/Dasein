package com.latticeengines.app.exposed.service.impl;

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
        List<ActivityAlert> alerts = activityAlertEntityMgr.findTopNAlertsByEntityId(internalAccountId,
                BusinessEntity.Account, version, category, max);
        if (CollectionUtils.isEmpty(alerts)) {
            log.warn(String.format(
                    "No Alerts found for the Account=%s ( internalId=%s ) Category=%s Version=%s Tenant=%s", accountId,
                    internalAccountId, category.name(), version, customerSpace));
            return new DataPage();
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

    private class ActivityAlertDTO {
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
            this.startTimestamp = Long
                    .valueOf((Integer) alert.getAlertData().get(ActivityStoreConstants.Alert.COL_START_TIMESTAMP));
            this.endTimestamp = Long
                    .valueOf((Integer) alert.getAlertData().get(ActivityStoreConstants.Alert.COL_END_TIMESTAMP));
        }
    }

}
