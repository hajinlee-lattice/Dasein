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
    Opportunity;

    private static EnumSet<LogicalDataType> typesExcludedFromRealTimeMetadata = EnumSet.of(LogicalDataType.InternalId,
            LogicalDataType.Event, LogicalDataType.Opportunity);

    public static boolean isExcludedFromRealTimeMetadata(LogicalDataType type) {
        return typesExcludedFromRealTimeMetadata.contains(type);
    }

    private static EnumSet<LogicalDataType> acausalDataTypes = EnumSet.of(LogicalDataType.Event,
            LogicalDataType.StageName);

    public static boolean isEventTypeOrDerviedFromEventType(LogicalDataType type) {
        return acausalDataTypes.contains(type);
    }

}
