package com.latticeengines.app.exposed.service.impl;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.app.exposed.service.ActivityTimelineService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;

@Component("activityTimelineService")
public class ActivityTimelineServiceImpl implements ActivityTimelineService {
    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineServiceImpl.class);

    @Inject
    private DataLakeService dataLakeService;

    @Inject
    private ActivityProxy activityProxy;

    @Value("${app.timeline.default.period}")
    private String defaultTimelinePeriod;

    private static final String componentName = "CDL";

    @Override
    public DataPage getAccountActivities(String accountId, String timelinePeriod, Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        timelinePeriod = StringUtils.isNotBlank(timelinePeriod) ? timelinePeriod : defaultTimelinePeriod;

        String internalAccountId = dataLakeService.getInternalAccountId(accountId, orgInfo);
        if (StringUtils.isBlank(internalAccountId)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "Unable to find any account in Atlas by accountid/lookupid of %s, customerSpace: %s",
                            accountId, customerSpace) });
        }

        ActivityTimelineQuery query = new ActivityTimelineQuery();
        query.setMainEntity(BusinessEntity.Account);
        query.setEntityId(internalAccountId);
        Pair<Instant, Instant> timeWindow = getTimeWindowFromPeriod(customerSpace, timelinePeriod);
        query.setStartTimeStamp(timeWindow.getLeft());
        query.setEndTimeStamp(timeWindow.getRight());

        return activityProxy.getData(customerSpace, null, query);
    }

    @Override
    public DataPage getContactActivities(String accountId, String contactId, String timelinePeriod,
            Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        timelinePeriod = StringUtils.isNotBlank(timelinePeriod) ? timelinePeriod : defaultTimelinePeriod;

        // Internal AccountId isn't needed for contact activity queries right now
        // Nevertheless look it up to avoid servicing data for invalie
        // accountid-contactid pairs
        String internalAccountId = dataLakeService.getInternalAccountId(accountId, orgInfo);
        if (StringUtils.isBlank(internalAccountId)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "Unable to find any account in Atlas by accountid/lookupid of %s, customerSpace: %s",
                            accountId, customerSpace) });
        }

        ActivityTimelineQuery query = new ActivityTimelineQuery();
        query.setMainEntity(BusinessEntity.Contact);
        query.setEntityId(contactId);
        Pair<Instant, Instant> timeWindow = getTimeWindowFromPeriod(customerSpace, timelinePeriod);
        query.setStartTimeStamp(timeWindow.getLeft());
        query.setEndTimeStamp(timeWindow.getRight());

        return activityProxy.getData(customerSpace, null, query);
    }

    private Pair<Instant, Instant> getTimeWindowFromPeriod(String customerSpace, String timelinePeriod) {
        try {
            Period expirationPeriod = Period.parse(timelinePeriod);
            Instant now = getCurrentInstant(customerSpace);
            return Pair.of(
                    now.atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).minus(expirationPeriod).toInstant(),
                    now.atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toInstant());
        } catch (DateTimeParseException exp) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "Unable to parse the time period for the timeline query:%s , customerSpace: %s ",
                            timelinePeriod, customerSpace) });
        }
    }

    private Instant getCurrentInstant(String customerSpace) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            String fakeCurrentDate = "";
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                    CustomerSpace.parse(customerSpace), componentName);
            Path fakeCurrentDatePath = cdlPath.append("FakeCurrentDate");
            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(fakeCurrentDatePath)) {
                fakeCurrentDate = camille.get(fakeCurrentDatePath).getData();
                timer.setTimerMessage(
                        "Retrieved FakeCurrentDate(" + fakeCurrentDate + ") from ZK for tenant: " + customerSpace);
            } else {
                timer.setTimerMessage("FakeCurrentDate not found in ZK for tenant: " + customerSpace);
            }
            return LocalDate.parse(fakeCurrentDate, DateTimeFormatter.ISO_DATE).atStartOfDay(ZoneOffset.UTC)
                    .toOffsetDateTime().toInstant();
        } catch (Exception e) {
            log.warn("Failed to get FakeCurrentDate from ZK for " + customerSpace + ". Using current timestamp instead",
                    e);
            return Instant.now();
        }
    }

    @VisibleForTesting
    void setDataLakeService(DataLakeService dataLakeService) {
        this.dataLakeService = dataLakeService;
    }

    @VisibleForTesting
    void setActivityProxy(ActivityProxy activityProxy) {
        this.activityProxy = activityProxy;
    }
}
