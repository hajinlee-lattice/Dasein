package com.latticeengines.app.exposed.service.impl;

import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.BUYING_STAGE_THRESHOLD;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.STAGE_BUYING;
import static com.latticeengines.domain.exposed.cdl.activity.ActivityStoreConstants.DnbIntent.STAGE_RESEARCHING;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.cdl.activity.ActivityTimelineMetrics;
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

    private static final String messageNoDataSource = "N/A";
    private static final String BUYING = "BUYING";
    private static final String RESEARCHING = "RESEARCHING";

    private static final ArrayList<String> STAGES = new ArrayList<>(
            Arrays.asList("Closed Won", "Closed", "Closed Lost"));

    private static Map<ActivityTimelineMetrics.MetricsType, List<AtlasStream.StreamType>> streamTypeListMap = new HashMap<ActivityTimelineMetrics.MetricsType, List<AtlasStream.StreamType>>() {
        {
            put(ActivityTimelineMetrics.MetricsType.NewActivities, Arrays.asList(AtlasStream.StreamType.WebVisit));
            put(ActivityTimelineMetrics.MetricsType.Newengagements,
                    Arrays.asList(AtlasStream.StreamType.MarketingActivity, AtlasStream.StreamType.Opportunity));
            put(ActivityTimelineMetrics.MetricsType.NewOpportunities,
                    Arrays.asList(AtlasStream.StreamType.Opportunity));
        }
    };

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
    public DataPage getContactActivities(String accountId, String contactId, String timelinePeriodStr,
            Set<AtlasStream.StreamType> streamTypes, Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        Period timelinePeriod = parseTimePeriod(customerSpace,
                StringUtils.isNotBlank(timelinePeriodStr) ? timelinePeriodStr : defaultTimelinePeriod);

        // Internal AccountId isn't needed for contact activity queries right now
        // Nevertheless look it up to avoid servicing data for invalie
        // accountid-contactid pairs
        String internalAccountId = getInternalAccountId(accountId, orgInfo, customerSpace);

        ActivityTimelineQuery query = buildActivityTimelineQuery(BusinessEntity.Contact, contactId, customerSpace,
                timelinePeriod);

        return filterStreamData(activityProxy.getData(customerSpace, null, query), streamTypes);
    }

    @Override
    public List<ActivityTimelineMetrics> getActivityTimelineMetrics(String accountId, String timelinePeriodStr,
            Map<String, String> orgInfo) {

        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        log.info(String.format("Retrieving Activity Timeline Metrics | tenant=%s",
                CustomerSpace.shortenCustomerSpace(customerSpace)));

        Period timelinePeriod = parseTimePeriod(customerSpace,
                StringUtils.isNotBlank(timelinePeriodStr) ? timelinePeriodStr : defaultActivityMetricsPeriod);
        DataPage data = getAccountActivities(customerSpace, accountId, timelinePeriod, orgInfo);

        DataPage contactData = dataLakeService.getAllContactsByAccountId(accountId, orgInfo);
        Instant cutoffTimeStamp = getTimeWindowFromPeriod(customerSpace, timelinePeriod).getLeft();

        DataPage intentData = new DataPage();
        intentData.setData(data.getData());
        String accountIntent = getAccountIntent(intentData);

        int newIdentifiedContactsCount = (int) dataFilter(data, AtlasStream.StreamType.MarketingActivity,
                SourceType.MARKETO).stream().filter(t -> t.get(InterfaceName.EventType.name()).equals("New Lead"))
                        .map(t -> t.get(InterfaceName.ContactId.name())).distinct().count();

        int newActivitiesCount = dataFilter(data, AtlasStream.StreamType.WebVisit, null).size();
        int newContactsCount = contactData.getData().stream()
                .filter(t -> t.get(InterfaceName.CDLCreatedTime.name()) != null)
                .filter(t -> (Long) t.get(InterfaceName.CDLCreatedTime.name()) >= cutoffTimeStamp.toEpochMilli())
                .collect(Collectors.toList()).size();
        newContactsCount += newIdentifiedContactsCount;
        int newEngagementsCount = dataFilter(data, AtlasStream.StreamType.Opportunity, null).size()
                + dataFilter(data, AtlasStream.StreamType.MarketingActivity, null).size();
        int newOpportunitiesCount = deduplicateOpportunityData(
                dataFilter(data, AtlasStream.StreamType.Opportunity, null)).stream()
                        .filter(t -> !STAGES.contains(t.get(InterfaceName.Detail1.name()))).collect(Collectors.toList())
                        .size();

        List<AtlasStream> streams = activityStoreProxy.getStreams(customerSpace);
        int days = timelinePeriod.getDays();

        List<ActivityTimelineMetrics> metrics = new ArrayList<ActivityTimelineMetrics>();
        metrics.add(getActivityTimelineMetrics(newActivitiesCount, ActivityTimelineMetrics.MetricsType.NewActivities,
                days, streams));
        metrics.add(getActivityTimelineMetrics(newContactsCount, ActivityTimelineMetrics.MetricsType.NewContacts, days,
                streams));
        metrics.add(getActivityTimelineMetrics(newEngagementsCount, ActivityTimelineMetrics.MetricsType.Newengagements,
                days, streams));
        metrics.add(getActivityTimelineMetrics(newOpportunitiesCount,
                ActivityTimelineMetrics.MetricsType.NewOpportunities, days, streams));
        metrics.add(
                new ActivityTimelineMetrics(accountIntent, ActivityTimelineMetrics.MetricsType.AccountIntent.getLabel(),
                        ActivityTimelineMetrics.MetricsType.AccountIntent.getDescription(null, days),
                        ActivityTimelineMetrics.MetricsType.AccountIntent.getContext(days)));

        return metrics;
    }

    private List<Map<String, Object>> deduplicateOpportunityData(List<Map<String, Object>> data) {
        Map<String, Map<String, Object>> opportunityMap = new HashMap<String, Map<String, Object>>();
        for (Map<String, Object> map : data) {
            String opportunityId = (String) map.get(InterfaceName.Detail2.name());
            if (opportunityId == null) {
                continue;
            }
            if (!opportunityMap.containsKey(opportunityId)) {
                opportunityMap.put(opportunityId, map);
                continue;
            }
            String oldStage = (String) opportunityMap.get(opportunityId).get(InterfaceName.Detail1.name());
            String newStage = (String) map.get(InterfaceName.Detail1.name());
            if (STAGES.indexOf(newStage) > STAGES.indexOf(oldStage)) {
                opportunityMap.put(opportunityId, map);
            }
        }
        return new ArrayList(opportunityMap.values());
    }

    private List<Map<String, Object>> getDeduplicateIntentData(DataPage data) {
        Map<String, Map<String, Object>> modelIntentMap = new HashMap<String, Map<String, Object>>();
        for (Map<String, Object> map : data.getData()) {
            String model = (String) map.get(InterfaceName.Detail1.name());
            if (modelIntentMap.containsKey(model)) {
                Long latest = (Long) modelIntentMap.get(model).get(InterfaceName.EventTimestamp.name());
                if (latest > (Long) map.get(InterfaceName.EventTimestamp.name())) {
                    continue;
                }
            }
            modelIntentMap.put(model, map);
        }
        return new ArrayList(modelIntentMap.values());
    }

    private String getAccountIntent(DataPage data) {
        String message = messageNoDataSource;
        data = filterStreamData(data, new HashSet<>(Arrays.asList(AtlasStream.StreamType.DnbIntentData)));
        for (Map<String, Object> map : getDeduplicateIntentData(data)) {
            String detail2 = (String) map.get(InterfaceName.Detail2.name());
            try {
                message = STAGE_BUYING.equals(message) ? STAGE_BUYING
                        : (Double.valueOf(detail2) >= BUYING_STAGE_THRESHOLD ? STAGE_BUYING : STAGE_RESEARCHING);
            } catch (Exception e) {
                log.warn("Fail getting Detail2 from DnbIntentData", e);
                break;
            }
        }
        return message;
    }

    private ActivityTimelineMetrics getActivityTimelineMetrics(Integer count,
            ActivityTimelineMetrics.MetricsType metricsType, Integer days, List<AtlasStream> streams) {

        List<AtlasStream.StreamType> streamTypes = streamTypeListMap.containsKey(metricsType)
                ? streamTypeListMap.get(metricsType)
                : new ArrayList<AtlasStream.StreamType>();

        String message = getMessage(streams, streamTypes, count);
        String description = getDescription(streams, streamTypes, count, days);
        String label = metricsType.getLabel();
        String context = metricsType.getContext(days);

        return new ActivityTimelineMetrics(message, label, description, context);
    }

    private String getMessage(List<AtlasStream> streams, List<AtlasStream.StreamType> streamTypes,
            Integer metricsCount) {
        if (!hasDataSources(streams, streamTypes) && !streamTypes.isEmpty()) {
            return messageNoDataSource;
        } else {
            return String.valueOf(metricsCount);
        }
    }

    private String getDescription(List<AtlasStream> streams, List<AtlasStream.StreamType> streamTypes,
            Integer metricsCount, Integer days) {
        if (!hasDataSources(streams, streamTypes) && !streamTypes.isEmpty()) {
            return ActivityTimelineMetrics.MetricsType.getDescription(null, days);
        } else {
            return ActivityTimelineMetrics.MetricsType.getDescription(metricsCount, days);
        }
    }

    private Boolean hasDataSources(List<AtlasStream> streams, List<AtlasStream.StreamType> streamTypes) {

        boolean hasDataSource = false;

        hasDataSource = streamTypes.stream().anyMatch(
                streamType -> streams.stream().filter(stream -> (stream.getStreamType() == streamType)).count() > 0);
        return hasDataSource;
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
        }
        return dataPage;
    }

    private Map<String, Object> getPrevailingJourneyStageEvent(String customerSpace, DataPage dataPage,
            Instant cutoffTimestamp) {
        String accountId = (String) dataPage.getData().get(0).get(InterfaceName.AccountId.name());

        Map<String, Object> prevailingStageEvent = dataPage.getData().stream() //
                .filter(event -> event.get(InterfaceName.StreamType.name())
                        .equals(AtlasStream.StreamType.JourneyStage.name())) //
                .filter(event -> (Long) event.get(InterfaceName.EventTimestamp.name()) <= cutoffTimestamp
                        .toEpochMilli()) //
                .max(Comparator.comparingLong(event -> (Long) event.get(InterfaceName.EventTimestamp.name())))
                .orElse(null);
        if (prevailingStageEvent == null) {
            prevailingStageEvent = getDefaultJourneyStageEvent(customerSpace, accountId,
                    cutoffTimestamp.toEpochMilli());
            if (prevailingStageEvent != null)
                dataPage.getData().add(prevailingStageEvent);
        } else {
            prevailingStageEvent.put(InterfaceName.EventTimestamp.name(), cutoffTimestamp.toEpochMilli());
        }
        return prevailingStageEvent;
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
        stageEvent.put(InterfaceName.EventType.name(),
                ActivityStoreConstants.JourneyStage.STREAM_EVENT_TYPE_JOURNEYSTAGECHANGE);
        stageEvent.put(InterfaceName.EventTimestamp.name(), eventTimestamp);
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
