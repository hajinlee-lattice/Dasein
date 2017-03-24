package com.latticeengines.domain.exposed.metadata;

import java.util.EnumSet;

public enum LogicalDataType {
    Id, //
    InternalId, //
    Date, //
    Event, //
    StageName, //
    Reference, //
    RowId, //
    Opportunity, //
    Metric, //
    Timestamp;

    private static EnumSet<LogicalDataType> typesExcludedFromRealTimeMetadata = EnumSet.of(
            LogicalDataType.InternalId, LogicalDataType.Event, LogicalDataType.Opportunity,
            LogicalDataType.Timestamp);

    public static boolean isExcludedFromRealTimeMetadata(LogicalDataType type) {
        return typesExcludedFromRealTimeMetadata.contains(type);
    }

    private static EnumSet<LogicalDataType> acausalDataTypes = EnumSet.of(LogicalDataType.Event,
            LogicalDataType.StageName);

    public static boolean isEventTypeOrDerviedFromEventType(LogicalDataType type) {
        return acausalDataTypes.contains(type);
    }

    private static EnumSet<LogicalDataType> systemGeneratedDataTypes = EnumSet
            .of(LogicalDataType.InternalId, LogicalDataType.RowId);

    public static boolean isSystemGeneratedEventType(LogicalDataType type) {
        return systemGeneratedDataTypes.contains(type);
    }

    private static EnumSet<LogicalDataType> typesExcludedFromFileScoringMapping = EnumSet
            .of(LogicalDataType.Event, LogicalDataType.Opportunity);

    public static boolean isExcludedFromScoringFileMapping(LogicalDataType type) {
        return typesExcludedFromFileScoringMapping.contains(type);
    }

}
