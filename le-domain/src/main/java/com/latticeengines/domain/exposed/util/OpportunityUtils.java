package com.latticeengines.domain.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.EntityType;

public final class OpportunityUtils {

    private static final Logger log = LoggerFactory.getLogger(OpportunityUtils.class);

    private static final String STREAM_NAME_FORMAT = "%s_%s"; // systemName_Opportunity

    protected OpportunityUtils() {
        throw new UnsupportedOperationException();
    }

    public static void setColumnMetadataUIProperties(@NotNull ColumnMetadata cm, @NotNull ActivityMetricsGroup group, boolean shouldAppendSystemName) {
        if (shouldAppendSystemName) {
            appendSystemName(cm, getSystemNameFromStreamName(group.getStream().getName()));
        }
    }

    public static String getStreamName(String systemName) {
        return String.format(STREAM_NAME_FORMAT, systemName, EntityType.Opportunity);
    }

    private static String getSystemNameFromStreamName(String streamName) {
        int idx = streamName.lastIndexOf('_');
        if (idx <= 0) {
            log.warn("Unable to get system name from stream name {}", streamName);
            return streamName;
        }
        return streamName.substring(0, idx);
    }

    private static void appendSystemName(ColumnMetadata cm, String systemName) {
        cm.setDisplayName(String.format("%s: %s", systemName, cm.getDisplayName()));
    }
}
