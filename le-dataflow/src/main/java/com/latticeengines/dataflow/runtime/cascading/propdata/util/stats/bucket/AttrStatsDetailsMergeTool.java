package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;

public interface AttrStatsDetailsMergeTool {

    AttributeStatsDetails merge(AttributeStatsDetails firstStatsDetails, AttributeStatsDetails secondStatsDetails,
            boolean printTop);

}
