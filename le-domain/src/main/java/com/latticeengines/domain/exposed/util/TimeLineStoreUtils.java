package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.EventFieldExtractor;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public final class TimeLineStoreUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeLineStoreUtils.class);
    private static final String RECORD_ID_PREFIX = "asr_";
    //TL_<TimelineID>_<Version>_<EntityId>
    private static final String PARTITIONKEY_FORMAT = "TL_%s_%s_%s";
    //<Timestamp>_<RecordId>
    private static final String SORTKEY_FORMAT = "%s_%s";

    public static final String ACCOUNT360_TIMELINE_NAME = "Account360";
    public static final String CONTACT360_TIMELINE_NAME = "Contact360";

    protected TimeLineStoreUtils() {
        throw new UnsupportedOperationException();
    }

    public static String contructTimelineId(String customerSpace, String timelineName) {
        log.info("contruct timeline {} in tenant {}.", timelineName, customerSpace);
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
        return String.format("%s_%s", tenantId, timelineName);
    }

    /**
     *streamType -> (desCol, srcColExtractor{srcCol, mappingType})
     */
    public static Map<String, Map<String, EventFieldExtractor>> getTimelineStandardMappings() {
        Map<String, Map<String, EventFieldExtractor>> timelineStandardMappings = new HashMap<>();
        for (AtlasStream.StreamType streamType : AtlasStream.StreamType.values()) {
            timelineStandardMappings.put(streamType.name(), getTimelineStandardMappingByStreamType(streamType));
        }
        return timelineStandardMappings;
    }

    public static Map<String, EventFieldExtractor> getTimelineStandardMappingByStreamType(AtlasStream.StreamType streamType) {
        Map<String, EventFieldExtractor> timelineStandardMapping = new HashMap<>();
        switch (streamType) {
            case WebVisit:
                timelineStandardMapping.put(TimelineStandardColumn.EventDate.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.WebVisitDate.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.AccountId.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.AccountId.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.EventType.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Constant).withMappingValue("Page Visit").build());
                timelineStandardMapping.put(TimelineStandardColumn.Detail1.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.WebVisitPageUrl.name()).build());
                break;
            case Opportunity:
                timelineStandardMapping.put(TimelineStandardColumn.EventDate.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.LastModifiedDate.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.AccountId.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.AccountId.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.EventType.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Constant).withMappingValue("Opportunity Update").build());
                timelineStandardMapping.put(TimelineStandardColumn.Detail1.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.StageName.name()).build());
                break;
            case MarketingActivity:
                timelineStandardMapping.put(TimelineStandardColumn.EventDate.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.ActivityDate.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.AccountId.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.AccountId.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.EventType.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.ActivityType.name()).build());
                timelineStandardMapping.put(TimelineStandardColumn.ContactId.getColumnName(),
                        new EventFieldExtractor.Builder().withMappingType(EventFieldExtractor.MappingType.Attribute).withMappingValue(InterfaceName.ContactId.name()).build());
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

    public static String generatePartitionKey(String version, String timelineId,
                                              String entityId) {
        return String.format(PARTITIONKEY_FORMAT, timelineId, version, entityId);
    }

    public static String generateSortKey(Long eventTimeStamp, String recordId) {
        return String.format(SORTKEY_FORMAT, String.valueOf(eventTimeStamp), recordId);
    }

    public enum TimelineStandardColumn {
        RecordId(InterfaceName.Id.name(), "String"),
        EventDate("EventTimestamp", "Long"),
        AccountId(InterfaceName.AccountId.name(), "String"),
        ContactId(InterfaceName.ContactId.name(), "String"),
        EventType("EventType", "String"),
        StreamType("StreamType", "String"),
        Detail1(InterfaceName.Detail1.name(), "String"),
        Detail2(InterfaceName.Detail2.name(), "String"),
        TrackedBySystem("Source", "String"),
        ContactName(InterfaceName.ContactName.name(), "String");

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
