package com.latticeengines.app.exposed.service.impl;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.cdl.ActivityStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;

@Component("activityTimelineService")
public class ActivityTimelineServiceImpl implements ActivityTimelineService {
    private static final Logger log = LoggerFactory.getLogger(ActivityTimelineServiceImpl.class);

    @Inject
    private DataLakeService dataLakeService;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Inject
    private ActivityProxy activityProxy;

    @Value("${app.timeline.default.back.period}")
    private String defaultBackPeriod;

    @Value("${app.timeline.default.period}")
    private String defaultTimelinePeriod;

    @Value("${app.timeline.activity.metrics.period}")
    private String defaultActivityMetricsPeriod;

    private static final String componentName = "CDL";

    @Override
    public DataPage getAccountActivities(String accountId, String timelinePeriodStr, String backPeriodStr,
            Set<AtlasStream.StreamType> streamTypes, Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        Period timelinePeriod = parseTimePeriod(customerSpace,
                StringUtils.isNotBlank(timelinePeriodStr) ? timelinePeriodStr : defaultTimelinePeriod);
        Period backPeriod = parseTimePeriod(customerSpace,
                StringUtils.isNotBlank(backPeriodStr) ? backPeriodStr : defaultBackPeriod);
        DataPage dataPage;
        if (streamTypes.contains(AtlasStream.StreamType.JourneyStage)) {
            dataPage = getAccountActivities(customerSpace, accountId, timelinePeriod.plus(backPeriod), orgInfo);
            dataPage = postProcessForJourneyStages(customerSpace, dataPage, timelinePeriod, streamTypes);
        } else {
            dataPage = getAccountActivities(customerSpace, accountId, timelinePeriod, orgInfo);
        }

        return filterStreamData(dataPage, streamTypes);
    }

    @Override
    public DataPage getContactActivities(String accountId, String contactId, String timelinePeriod,
            Set<AtlasStream.StreamType> streamTypes, Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        timelinePeriod = StringUtils.isNotBlank(timelinePeriod) ? timelinePeriod : defaultTimelinePeriod;

        // Internal AccountId isn't needed for contact activity queries right now
        // Nevertheless look it up to avoid servicing data for invalie
        // accountid-contactid pairs
        String internalAccountId = getInternalAccountId(accountId, orgInfo, customerSpace);

        ActivityTimelineQuery query = buildActivityTimelineQuery(BusinessEntity.Contact, contactId, customerSpace,
                parseTimePeriod(customerSpace, timelinePeriod));

        return filterStreamData(activityProxy.getData(customerSpace, null, query), streamTypes);
    }

    @Override
    public Map<String, Integer> getActivityTimelineMetrics(String accountId, String timelinePeriod,
            Map<String, String> orgInfo) {

        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        log.info(String.format("Retrieving Activity Timeline Metrics | tenant=%s",
                CustomerSpace.shortenCustomerSpace(customerSpace)));

        timelinePeriod = StringUtils.isNotBlank(timelinePeriod) ? timelinePeriod : defaultActivityMetricsPeriod;
        DataPage data = getAccountActivities(customerSpace, accountId, parseTimePeriod(customerSpace, timelinePeriod),
                orgInfo);

        Map<String, Integer> metrics = new HashMap<>();
        metrics.put("newActivities", dataFilter(data, AtlasStream.StreamType.WebVisit, null).size());

        metrics.put("newIdentifiedContacts",
                (int) dataFilter(data, AtlasStream.StreamType.MarketingActivity, SourceType.MARKETO).stream()
                        .filter(t -> t.get(InterfaceName.EventType.name()).equals("New Lead"))
                        .map(t -> t.get(InterfaceName.ContactId.name())).distinct().count());

        metrics.put("newEngagements", dataFilter(data, AtlasStream.StreamType.Opportunity, null).size()
                + dataFilter(data, AtlasStream.StreamType.MarketingActivity, null).size());

        metrics.put("newOpportunities",
                (int) dataFilter(data, AtlasStream.StreamType.Opportunity, null).stream()
                        .filter(t -> !t.get(InterfaceName.Detail1.name()).equals("Closed")
                                && !t.get(InterfaceName.Detail1.name()).equals("Closed Won"))
                        .count());
        return metrics;
    }

    private DataPage filterStreamData(DataPage dataPage, Set<AtlasStream.StreamType> streamTypes) {
        Set<String> streamTypeStrs = streamTypes.stream().map(AtlasStream.StreamType::name) //
                .collect(Collectors.toSet());
        dataPage.setData(dataPage.getData().stream()
                .filter(event -> streamTypeStrs.contains((String) event.get(InterfaceName.StreamType.name())))
                .collect(Collectors.toList()));
        return dataPage;
    }

    private DataPage getAccountActivities(String customerSpace, String accountId, Period timelinePeriod,
            Map<String, String> orgInfo) {
        String internalAccountId = getInternalAccountId(accountId, orgInfo, customerSpace);
        ActivityTimelineQuery query = buildActivityTimelineQuery(BusinessEntity.Account, internalAccountId,
                customerSpace, timelinePeriod);

        return activityProxy.getData(customerSpace, null, query);
    }

    private DataPage postProcessForJourneyStages(String customerSpace, DataPage dataPage, Period timelinePeriod,
            Set<AtlasStream.StreamType> streamTypes) {
        if (dataPage == null || CollectionUtils.isEmpty(dataPage.getData()))
            return dataPage;

        if (streamTypes.contains(AtlasStream.StreamType.JourneyStage)) {
            Instant cutoffTimeStamp = getTimeWindowFromPeriod(customerSpace, timelinePeriod).getLeft();
            Map<String, Object> datum = getPrevailingJourneyStageEvent(customerSpace, dataPage, cutoffTimeStamp);

            dataPage.setData(dataPage.getData().stream().filter(
                    event -> (Long) event.get(InterfaceName.EventTimestamp.name()) >= cutoffTimeStamp.toEpochMilli())
                    .collect(Collectors.toList()));

            if (MapUtils.isNotEmpty(datum))
                dataPage.getData().add(datum);
        }
        return dataPage;
    }

    private Map<String, Object> getPrevailingJourneyStageEvent(String customerSpace, DataPage dataPage,
            Instant cutoffTimestamp) {
        String accountId = (String) dataPage.getData().get(0).get(InterfaceName.AccountId.name());
        return dataPage.getData().stream() //
                .filter(event -> event.get(InterfaceName.StreamType.name())
                        .equals(AtlasStream.StreamType.JourneyStage.name())) //
                .filter(event -> (Long) event.get(InterfaceName.EventTimestamp.name()) <= cutoffTimestamp
                        .toEpochMilli()) //
                .max(Comparator.comparingLong(event -> (Long) event.get(InterfaceName.EventTimestamp.name()))) //
                .orElse(getDefaultJourneyStageEvent(customerSpace, accountId, cutoffTimestamp.toEpochMilli()));
    }

    private Map<String, Object> getDefaultJourneyStageEvent(String customerSpace, String accountId,
            long eventTimestamp) {
        List<JourneyStage> stages = activityStoreProxy.getJourneyStages(customerSpace);
        if (CollectionUtils.isEmpty(stages)) {
            log.warn("No Journey Stage Config found for customerSpace=" + customerSpace);
            return null;
        }

        JourneyStage defaultStageConfig = Collections.min(stages, Comparator.comparing(JourneyStage::getPriority));
        Map<String, Object> stageEvent = new HashMap<>();

        stageEvent.put(InterfaceName.StreamType.name(), AtlasStream.StreamType.JourneyStage.name());
        stageEvent.put(InterfaceName.AccountId.name(), accountId);
        stageEvent.put(InterfaceName.Detail1.name(), defaultStageConfig.getStageName());
        stageEvent.put(InterfaceName.EventTimestamp.name(), eventTimestamp);
        stageEvent.put(InterfaceName.EventType.name(),
                ActivityStoreConstants.JourneyStage.STREAM_EVENT_TYPE_JOURNEYSTAGECHANGE);
        stageEvent.put(InterfaceName.Source.name(), ActivityStoreConstants.JourneyStage.STREAM_SOURCE_ATLAS);
        return stageEvent;
    }

    private String getInternalAccountId(String accountId, Map<String, String> orgInfo, String customerSpace) {
        String internalAccountId = dataLakeService.getInternalAccountId(accountId, orgInfo);
        if (StringUtils.isBlank(internalAccountId)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "Unable to find any account in Atlas by accountid/lookupid of %s, customerSpace: %s",
                            accountId, customerSpace) });
        }
        return internalAccountId;
    }

    private List<Map<String, Object>> dataFilter(DataPage dataPage, AtlasStream.StreamType streamType,
            SourceType sourceType) {
        List<Map<String, Object>> result = dataPage.getData();
        if (streamType != null) {
            result = result.stream().filter(t -> t.get(InterfaceName.StreamType.name()).equals(streamType.name()))
                    .collect(Collectors.toList());
        }
        if (sourceType != null) {
            result = result.stream().filter(t -> t.get(InterfaceName.Source.name()).equals(sourceType.getName()))
                    .collect(Collectors.toList());
        }
        return result;
    }

    private ActivityTimelineQuery buildActivityTimelineQuery(BusinessEntity entity, String entityId,
            String customerSpace, Period timelinePeriod) {

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

    private Period parseTimePeriod(String customerSpace, String timelinePeriod) {
        try {
            return Period.parse(timelinePeriod);
        } catch (DateTimeParseException exp) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "Unable to parse the time period for the timeline query:%s , customerSpace: %s ",
                            timelinePeriod, customerSpace) });
        }
    }

    private Pair<Instant, Instant> getTimeWindowFromPeriod(String customerSpace, Period timelinePeriod) {
        Instant now = getCurrentInstant(customerSpace);
        return Pair.of(now.atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).minus(timelinePeriod).toInstant(),
                now.atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toInstant());
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
            log.warn(
                    "Failed to get FakeCurrentDate from ZK for " + customerSpace + ". Using current timestamp instead");
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

    @VisibleForTesting
    void setActivityStoreProxy(ActivityStoreProxy activityStoreProxy) {
        this.activityStoreProxy = activityStoreProxy;
    }

}
