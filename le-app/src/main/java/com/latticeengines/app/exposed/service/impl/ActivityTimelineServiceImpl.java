package com.latticeengines.app.exposed.service.impl;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.eai.SourceType;
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

    @Value("${app.timeline.activity.metrics.period}")
    private String defaultActivityMetricsPeriod;

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

        ActivityTimelineQuery query = buildActivityTimelineQuery(BusinessEntity.Account, internalAccountId,
                customerSpace, timelinePeriod);

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

        ActivityTimelineQuery query = buildActivityTimelineQuery(BusinessEntity.Contact, contactId, customerSpace,
                timelinePeriod);

        return activityProxy.getData(customerSpace, null, query);
    }

    @Override
    public Map<String, Integer> getActivityTimelineMetrics(String accountId, String timelinePeriod,
            Map<String, String> orgInfo) {

        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        log.info(String.format("Retrieving Activity Timeline Metrics | tenant=%s",
                CustomerSpace.shortenCustomerSpace(customerSpace)));

        timelinePeriod = StringUtils.isNotBlank(timelinePeriod) ? timelinePeriod : defaultActivityMetricsPeriod;
        DataPage data = getAccountActivities(accountId, timelinePeriod, orgInfo);

        Map<String, Integer> metrics = new HashMap<String, Integer>();
        metrics.put("newActivities", dataFilter(data, AtlasStream.StreamType.WebVisit, null).size());
        metrics.put("newIdentifiedContacts",
                dataFilter(data, AtlasStream.StreamType.MarketingActivity, SourceType.MARKETO).stream()
                        .filter(t -> t.get("EventType").equals("New Lead")).map(t -> t.get("ContactId")).distinct()
                        .collect(Collectors.toList()).size());
        metrics.put("newEngagements", dataFilter(data, AtlasStream.StreamType.Opportunity, null).size()
                + dataFilter(data, AtlasStream.StreamType.MarketingActivity, null).size());
        metrics.put("newOpportunities",
                dataFilter(data, AtlasStream.StreamType.Opportunity, null).stream()
                        .filter(t -> !t.get("Detail1").equals("Closed") && !t.get("Detail1").equals("Closed Won"))
                        .collect(Collectors.toList()).size());
        return metrics;
    }

    private List<Map<String, Object>> dataFilter(DataPage dataPage, AtlasStream.StreamType streamType,
            SourceType sourceType) {
        List<Map<String, Object>> result = dataPage.getData();
        if (streamType != null) {
            result = result.stream().filter(t -> t.get("StreamType").equals(streamType.name()))
                    .collect(Collectors.toList());
        }
        if (sourceType != null) {
            result = result.stream().filter(t -> t.get("Source").equals(sourceType.getName()))
                    .collect(Collectors.toList());
        }
        return result;
    }

    private ActivityTimelineQuery buildActivityTimelineQuery(BusinessEntity entity, String entityId,
            String customerSpace, String timelinePeriod) {

        ActivityTimelineQuery query = new ActivityTimelineQuery();
        query.setMainEntity(entity);
        query.setEntityId(entityId);
        Pair<Instant, Instant> timeWindow = getTimeWindowFromPeriod(customerSpace, timelinePeriod);
        query.setStartTimeStamp(timeWindow.getLeft());
        query.setEndTimeStamp(timeWindow.getRight());

        log.info(String.format("Retrieving %s activity data using query: %s | tenant=%s", entity.name(),
                JsonUtils.serialize(query), CustomerSpace.shortenCustomerSpace(customerSpace)));

        return query;
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
        try {
            String fakeCurrentDate;
            Path cdlPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                    CustomerSpace.parse(customerSpace), componentName);
            Path fakeCurrentDatePath = cdlPath.append("FakeCurrentDate");
            Camille camille = CamilleEnvironment.getCamille();
            fakeCurrentDate = camille.get(fakeCurrentDatePath).getData();
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
