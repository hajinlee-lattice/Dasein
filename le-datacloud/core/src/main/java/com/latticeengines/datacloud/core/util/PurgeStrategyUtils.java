package com.latticeengines.datacloud.core.util;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER_LOOKUP;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.DUNS_GUIDE_BOOK;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

public class PurgeStrategyUtils {
    private static Set<String> amSourceTypes = ImmutableSet.of(ACCOUNT_MASTER, ACCOUNT_MASTER_LOOKUP, DUNS_GUIDE_BOOK);

    public static SourceType getSourceType(Source source) {
        if (source instanceof IngestionSource) {
            return SourceType.INGESTION_SOURCE;
        }
        if (amSourceTypes.contains(source.getSourceName())) {
            return SourceType.AM_SOURCE;
        }
        return SourceType.GENERAL_SOURCE;
    }

}
