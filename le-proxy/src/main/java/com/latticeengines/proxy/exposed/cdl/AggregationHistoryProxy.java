package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.cdl.AggregationHistory;

public interface AggregationHistoryProxy {

    AggregationHistory create(String customerSpace, AggregationHistory aggregationHistory);

}
