package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;

public interface AttrStatsDetailsMergeTool {

    AttributeStats merge(AttributeStats firstStatsDetails, AttributeStats secondStatsDetails,
                         boolean printTop);

}
