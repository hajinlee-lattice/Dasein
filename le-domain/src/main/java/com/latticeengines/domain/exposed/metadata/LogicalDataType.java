package com.latticeengines.domain.exposed.metadata;

import java.util.EnumSet;

public enum LogicalDataType {
    Id, //
    InternalId, //
    Date, //
    Event, //
    Reference, //
    RowId, //
    Opportunity;

    private static EnumSet<LogicalDataType> typesExcludedFromRealTimeMetadata = EnumSet.of(LogicalDataType.InternalId,
            LogicalDataType.Event, LogicalDataType.Opportunity);

    public static boolean isExcludedFromRealTimeMetadata(LogicalDataType type) {
        return typesExcludedFromRealTimeMetadata.contains(type);
    }
}
