package com.latticeengines.proxy.exposed.objectapi;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface RatingProxy {

    DataPage getData(String customerSpace, FrontEndQuery frontEndQuery);

    Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getData(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    // cached
    Map<String, Long> getCoverage(String customerSpace, FrontEndQuery frontEndQuery);

}
