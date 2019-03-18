package com.latticeengines.proxy.exposed.objectapi;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;

public interface EntityProxy {

    // cached
    Long getCount(String customerSpace, FrontEndQuery frontEndQuery);

    Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery);

    Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    // cached
    DataPage getData(String customerSpace, FrontEndQuery frontEndQuery);

    DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery);

    DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, Version version,
            boolean enforceTranslation);

    // cached
    Map<String, Long> getRatingCount(String customerSpace, RatingEngineFrontEndQuery frontEndQuery);

    // cached
    DataPage getProducts(String customerSpace);

    DataPage getProductsFromObjectApi(String customerSpace, DataCollection.Version version);

}
