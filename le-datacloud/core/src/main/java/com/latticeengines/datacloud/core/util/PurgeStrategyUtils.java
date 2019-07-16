package com.latticeengines.datacloud.core.util;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

public class PurgeStrategyUtils {
    private static Set<String> amSourceTypes = ImmutableSet.of("AccountMaster", "AccountMasterLookup", "DunsGuideBook");

    public static SourceType getSourceType(Source source) {///
        if (source instanceof IngestionSource) {
            return SourceType.INGESTION_SOURCE;
        }
        if (amSourceTypes.contains(source.getSourceName())) {
            return SourceType.AM_SOURCE;
        }
        return SourceType.GENERAL_SOURCE;
    }

}
