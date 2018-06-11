package com.latticeengines.proxy.exposed.objectapi;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;

public interface EntityProxy {

    Long getCount(String customerSpace, FrontEndQuery frontEndQuery);
    Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery);
    Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getData(String customerSpace, FrontEndQuery frontEndQuery);
    DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery);
    DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    Map<String, Long> getRatingCount(String customerSpace, RatingEngineFrontEndQuery frontEndQuery);

}
