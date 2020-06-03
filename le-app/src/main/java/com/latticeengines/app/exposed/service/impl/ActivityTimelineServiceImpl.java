package com.latticeengines.app.exposed.service.impl;

import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
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
    private ActivityProxy activityProxy;

    @Inject
    private ActivityStoreProxy activityStoreProxy;

    @Value("${app.timeline.default.period}")
    private String defaultTimelinePeriod;

    private static class ActivityInterfaceName {
        public static final String RecordId = "recordId";
        public static final String AccountId = "accountId";
        public static final String SortKey = "sortKey";
        public static final String PartitionKey = "partitionKey";
        public static final String ContactId = "contactId";
        public static final String ContactName = "contactName";
        public static final String EventType = "eventType";
        public static final String Detail1 = "detail1";
        public static final String EventTimestamp = "eventTimestamp";
        public static final String ActivityCount = "ActivityCount";
        public static final String AnonymousContactName = "Anonymous";
        public static final String AllItemName = "All";
    }

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

    @Override
    public DataPage getAccountAggregationReportByContact(String accountId, String timelinePeriod,
            Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        DataPage accountActivityData = getAccountActivities(accountId, timelinePeriod, orgInfo);
        if (CollectionUtils.isEmpty(accountActivityData.getData())) {
            log.info(String.format("No Activity data found for accountId:%s for timeline:%s | CustomerSpace: %s",
                    accountId, timelinePeriod, customerSpace));
            return new DataPage(new ArrayList<>());
        }

        Map<String, Long> contactsLookup = dataLakeService.getAllContactsByAccountId(accountId, orgInfo).getData()
                .stream()
                .collect(Collectors.toMap(m -> (String) m.get(InterfaceName.ContactId.name()),
                        m -> (Long) m.getOrDefault(InterfaceName.LastActivityDate.name(),
                                m.get(InterfaceName.CDLUpdatedTime.name()))));
        Map<String, Map<String, Object>> data = new HashMap<>();

        Map<String, Object> anonymousContactDataItem = new HashMap<>();
        anonymousContactDataItem.put(ActivityInterfaceName.ContactName, ActivityInterfaceName.AnonymousContactName);
        anonymousContactDataItem.put(ActivityInterfaceName.ActivityCount, 0L);
        anonymousContactDataItem.put(InterfaceName.LastActivityDate.name(), 0L);

        Map<String, Object> allContactDataItem = new HashMap<>();
        allContactDataItem.put(ActivityInterfaceName.ContactName, ActivityInterfaceName.AllItemName);
        allContactDataItem.put(ActivityInterfaceName.ActivityCount, 0L);
        allContactDataItem.put(InterfaceName.LastActivityDate.name(), 0L);

        for (Map<String, Object> row : accountActivityData.getData()) {
            String id = row.containsKey(ActivityInterfaceName.ContactId)
                    ? (String) row.get(ActivityInterfaceName.ContactId)
                    : null;
            Map<String, Object> dataItem;
            if (StringUtils.isNotBlank(id)) {
                dataItem = data.containsKey(id) ? data.get(id) : new HashMap<>();
                dataItem.put(ActivityInterfaceName.ContactId, id);
                dataItem.put(ActivityInterfaceName.ContactName,
                        row.getOrDefault(ActivityInterfaceName.ContactName, ""));
                dataItem.put(InterfaceName.LastActivityDate.name(), contactsLookup.getOrDefault(id, 0L));
            } else {
                dataItem = anonymousContactDataItem;
                dataItem.put(InterfaceName.LastActivityDate.name(),
                        Math.max((Long) dataItem.get(InterfaceName.LastActivityDate.name()),
                                (Long) row.get(ActivityInterfaceName.EventTimestamp)));
            }
            dataItem.put(ActivityInterfaceName.ActivityCount,
                    (Long) dataItem.getOrDefault(ActivityInterfaceName.ActivityCount, 0L) + 1);

            allContactDataItem.put(ActivityInterfaceName.ActivityCount,
                    (Long) allContactDataItem.getOrDefault(ActivityInterfaceName.ActivityCount, 0L) + 1);
            allContactDataItem.put(InterfaceName.LastActivityDate.name(),
                    Math.max((Long) allContactDataItem.get(InterfaceName.LastActivityDate.name()),
                            (Long) row.get(ActivityInterfaceName.EventTimestamp)));

            data.put(id, dataItem);
        }
        List<Map<String, Object>> toReturn = new ArrayList<>();
        toReturn.add(allContactDataItem);
        List<Map<String, Object>> aggregatedContactsData = new ArrayList<>(data.values());
        aggregatedContactsData.sort(Comparator.comparing(o -> ((Long) o.get(ActivityInterfaceName.ActivityCount))));
        Collections.reverse(aggregatedContactsData);
        toReturn.addAll(aggregatedContactsData);

        return new DataPage(toReturn);
    }

    @Override
    public DataPage getAccountAggregationReportByProductInterest(String accountId, String timelinePeriod,
            Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).getTenantId();
        DataPage accountActivityData = getAccountActivities(accountId, timelinePeriod, orgInfo);
        if (CollectionUtils.isEmpty(accountActivityData.getData())) {
            log.info(String.format("No Activity data found for accountId:%s for timeline:%s | CustomerSpace: %s",
                    accountId, timelinePeriod, customerSpace));
            return new DataPage(new ArrayList<>());
        }
        Map<String, DimensionMetadata> dimensionMetadata = activityStoreProxy
                .getDimensionMetadataInStream(customerSpace, AtlasStream.StreamType.WebVisit.name(), null);
        if (MapUtils.isEmpty(dimensionMetadata) || !dimensionMetadata.containsKey(InterfaceName.PathPatternId.name())
                || CollectionUtils
                        .isEmpty(dimensionMetadata.get(InterfaceName.PathPatternId.name()).getDimensionValues())) {
            log.info(String.format("No Webvisit steam metadata for CustomerSpace: %s", customerSpace));
            return new DataPage(new ArrayList<>());
        }

        List<Map<String, Object>> pathPatterns = dimensionMetadata.get(InterfaceName.PathPatternId.name())
                .getDimensionValues();
        pathPatterns.forEach(p -> p.put(InterfaceName.PathPattern.name(),
                fixRegex((String) p.get(InterfaceName.PathPattern.name()))));
        Map<String, String> pathPatternMap = pathPatterns.stream()
                .collect(Collectors.toMap(p -> (String) p.get(InterfaceName.PathPattern.name()),
                        p -> (String) p.get(InterfaceName.PathPatternName.name())));

        Map<String, Map<String, Object>> data = new HashMap<>();
        Map<String, Object> allDataItem = new HashMap<>();
        allDataItem.put(InterfaceName.PathPatternName.name(), ActivityInterfaceName.AllItemName);
        allDataItem.put(ActivityInterfaceName.ActivityCount, 0L);
        allDataItem.put(InterfaceName.LastActivityDate.name(), 0L);

        for (Map<String, Object> row : accountActivityData.getData()) {
            if (!row.containsKey(ActivityInterfaceName.Detail1))
                continue;

            String pathPatterName = getPathPatternName((String) row.get(ActivityInterfaceName.Detail1), pathPatternMap);
            if (StringUtils.isBlank(pathPatterName))
                continue;

            Map<String, Object> dataItem;

            dataItem = data.containsKey(pathPatterName) ? data.get(pathPatterName) : new HashMap<>();
            dataItem.put(InterfaceName.PathPatternName.name(), pathPatterName);
            dataItem.put(InterfaceName.LastActivityDate.name(),
                    Math.max((Long) dataItem.getOrDefault(InterfaceName.LastActivityDate.name(), 0L),
                            (Long) row.get(ActivityInterfaceName.EventTimestamp)));

            dataItem.put(ActivityInterfaceName.ActivityCount,
                    (Long) dataItem.getOrDefault(ActivityInterfaceName.ActivityCount, 0L) + 1);

            allDataItem.put(ActivityInterfaceName.ActivityCount,
                    (Long) allDataItem.get(ActivityInterfaceName.ActivityCount) + 1);
            allDataItem.put(InterfaceName.LastActivityDate.name(),
                    Math.max((Long) allDataItem.get(InterfaceName.LastActivityDate.name()),
                            (Long) row.get(ActivityInterfaceName.EventTimestamp)));

            data.put(pathPatterName, dataItem);
        }
        List<Map<String, Object>> toReturn = new ArrayList<>();
        toReturn.add(allDataItem);
        List<Map<String, Object>> aggregatedEventTypeData = new ArrayList<>(data.values());
        aggregatedEventTypeData.sort(Comparator.comparing(o -> ((Long) o.get(ActivityInterfaceName.ActivityCount))));
        Collections.reverse(aggregatedEventTypeData);
        toReturn.addAll(aggregatedEventTypeData);
        return new DataPage(toReturn);

    }

    private String fixRegex(String path) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '*' && (i == 0 || path.charAt(i - 1) != '.')) {
                sb.append('.');
            }
            sb.append(path.charAt(i));
        }
        return sb.toString();
    }

    private String getPathPatternName(String path, Map<String, String> pathPatternMap) {
        for (String pathpattern : pathPatternMap.keySet()) {
            try {
                Pattern pattern = Pattern.compile(pathpattern);
                if (pattern.matcher(path).matches())
                    return pathPatternMap.get(pathpattern);
            } catch (PatternSyntaxException parseSyntaxException) {
                log.error("Unable to parse pattern " + pathpattern);
            }
        }
        return null;
    }

    @Override
    public DataPage getAccountAggregationReportByEventType(String accountId, String timelinePeriod,
            Map<String, String> orgInfo) {
        DataPage accountActivityData = getAccountActivities(accountId, timelinePeriod, orgInfo);
        if (CollectionUtils.isEmpty(accountActivityData.getData())) {
            log.info(String.format("No Activity data found for accountId:%s for timeline:%s", accountId,
                    timelinePeriod));
            return new DataPage(new ArrayList<>());
        }

        Map<String, Map<String, Object>> data = new HashMap<>();
        Map<String, Object> allDataItem = new HashMap<>();
        allDataItem.put(ActivityInterfaceName.EventType, ActivityInterfaceName.AllItemName);
        allDataItem.put(ActivityInterfaceName.ActivityCount, 0L);
        allDataItem.put(InterfaceName.LastActivityDate.name(), 0L);

        for (Map<String, Object> row : accountActivityData.getData()) {
            String eventType = (String) row.get(ActivityInterfaceName.EventType);
            Map<String, Object> dataItem;

            dataItem = data.containsKey(eventType) ? data.get(eventType) : new HashMap<>();
            dataItem.put(ActivityInterfaceName.EventType, eventType);
            dataItem.put(InterfaceName.LastActivityDate.name(),
                    Math.max((Long) dataItem.getOrDefault(InterfaceName.LastActivityDate.name(), 0L),
                            (Long) row.get(ActivityInterfaceName.EventTimestamp)));

            dataItem.put(ActivityInterfaceName.ActivityCount,
                    (Long) dataItem.getOrDefault(ActivityInterfaceName.ActivityCount, 0L) + 1);

            allDataItem.put(ActivityInterfaceName.ActivityCount,
                    (Long) allDataItem.get(ActivityInterfaceName.ActivityCount) + 1);
            allDataItem.put(InterfaceName.LastActivityDate.name(),
                    Math.max((Long) allDataItem.get(InterfaceName.LastActivityDate.name()),
                            (Long) row.get(ActivityInterfaceName.EventTimestamp)));

            data.put(eventType, dataItem);
        }
        List<Map<String, Object>> toReturn = new ArrayList<>();
        toReturn.add(allDataItem);
        List<Map<String, Object>> aggregatedEventTypeData = new ArrayList<>(data.values());
        aggregatedEventTypeData.sort(Comparator.comparing(o -> ((Long) o.get(ActivityInterfaceName.ActivityCount))));
        Collections.reverse(aggregatedEventTypeData);
        toReturn.addAll(aggregatedEventTypeData);

        return new DataPage(toReturn);
    }

    private Pair<Instant, Instant> getTimeWindowFromPeriod(String customerSpace, String timelinePeriod) {
        try {
            Period expirationPeriod = Period.parse(timelinePeriod);
            return Pair.of(
                    Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).minus(expirationPeriod)
                            .toInstant(),
                    Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toInstant());
        } catch (DateTimeParseException exp) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { String.format(
                            "Unable to parse the time period for the timeline query:%s , customerSpace: %s ",
                            timelinePeriod, customerSpace) });
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
