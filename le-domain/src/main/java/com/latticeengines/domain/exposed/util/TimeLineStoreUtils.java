package com.latticeengines.domain.exposed.util;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.EventTypeExtractor;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public final class TimeLineStoreUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeLineStoreUtils.class);
    private static final String RECORD_ID_PREFIX = "asr_";
    //TL_<Tenant>_<TimelineID>_<Version>_<EntityId>
    private static final String PARTITIONKEY_FORMAT = "TL_%s_%s_%s_%s";
    //<Timestamp>_<RecordId>
    private static final String SORTKEY_FORMAT = "%s_%s";

    protected TimeLineStoreUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     *streamType -> (desCol, srcColExtractor{srcCol, mappingType})
     */
    public static Map<AtlasStream.StreamType, Map<String, EventTypeExtractor>> getTimelineStandardMappings() {
        Map<AtlasStream.StreamType, Map<String, EventTypeExtractor>> timelineStandardMappings = new HashMap<>();
        for (AtlasStream.StreamType streamType : AtlasStream.StreamType.values()) {
            timelineStandardMappings.put(streamType, getTimelineStandardMappingByStreamType(streamType));
        }
        return timelineStandardMappings;
    }

    public static Map<String, EventTypeExtractor> getTimelineStandardMappingByStreamType(AtlasStream.StreamType streamType) {
        Map<String, EventTypeExtractor> timelineStandardMapping = new HashMap<>();
        switch (streamType) {
            case WebVisit:
                timelineStandardMapping.put(TimelineStandardColumn.EventDate.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.WebVisitDate.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.AccountId.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.AccountId.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.EventType.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Constant).withMappingValue("Page Visit").build());
                timelineStandardMapping.put(TimelineStandardColumn.ActivityDetail.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.WebVisitPageUrl.name()).build());
                break;
            case Opportunity:
                timelineStandardMapping.put(TimelineStandardColumn.EventDate.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.LastModifiedDate.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.AccountId.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.AccountId.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.EventType.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Constant).withMappingValue("Opportunity Update").build());
                timelineStandardMapping.put(TimelineStandardColumn.ActivityDetail.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.StageName.name()).build());
                break;
            case MarketingActivity:
                timelineStandardMapping.put(TimelineStandardColumn.EventDate.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.ActivityDate.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.AccountId.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.AccountId.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.EventType.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.ActivityType.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.ContactId.getColumnName(),
                        new EventTypeExtractor.Builder().withMappingType(EventTypeExtractor.MappingType.Attribute).withMappingValue(InterfaceName.ContactId.name()).build());
                break;
            default:break;
        }
        return timelineStandardMapping;
    }

    public static String generateRecordId() {
        String uuid;
        do {
            // try until uuid does not start with catalog prefix
            uuid = AvroUtils.getAvroFriendlyString(UuidUtils.shortenUuid(UUID.randomUUID()));
        } while (uuid.startsWith(RECORD_ID_PREFIX));
        return RECORD_ID_PREFIX + uuid;
    }

    public static String generatePartitionKey(String customerSpace, String timelineId, String version,
                                              String entityId) {
        return String.format(PARTITIONKEY_FORMAT, customerSpace, timelineId, version, entityId);
    }

    public static String generateSortKey(String recordId) {
        return String.format(SORTKEY_FORMAT, String.valueOf(getCurrentTimestamp()), recordId);
    }

    private static Long getCurrentTimestamp() {
        return Instant.now().toEpochMilli();
    }

    public enum TimelineStandardColumn {
        RecordId("recordId", "String"),
        EventDate("eventTimestamp", "Long"),
        AccountId("accountId", "String"),
        ContactId("contactId", "String"),
        EventType("eventType", "String"),
        ActivityDetail("Detail1", "String"),
        TrackedBySystem("source", "String"),
        ContactName("ContactName", "String");

        private String columnName;
        private String dataType;

        private static List<String> columnNames;
        static {
            columnNames = new ArrayList<>();
            for (TimelineStandardColumn entry : values()) {
                columnNames.add(entry.getColumnName());
            }
        }

        TimelineStandardColumn(String columnName, String dataType) {
            this.columnName = columnName;
            this.dataType = dataType;
        }

        public static List<String> getColumnNames() {
            return columnNames;
        }

        public static TimelineStandardColumn fromColumnNameToTimelineStandardColumn(String columnName) {
            for (TimelineStandardColumn entry : values()) {
                if (entry.getColumnName().equals(columnName)) {
                    return entry;
                }
            }
            return null;
        }

        public static String getDataTypeFromColumnName(String columnName) {
            for (TimelineStandardColumn entry : values()) {
                if (entry.getColumnName().equals(columnName)) {
                    return entry.getDataType();
                }
            }
            return null;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }
    }

}
