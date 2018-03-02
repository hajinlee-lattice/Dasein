package com.latticeengines.proxy.exposed.objectapi;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

import reactor.core.publisher.Mono;

public interface RatingProxy {

    DataPage getData(String customerSpace, FrontEndQuery frontEndQuery);

    Map<String, Long> getCoverage(String customerSpace, FrontEndQuery frontEndQuery);

    Long getCountFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getDataFromObjectApi(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

    Mono<DataPage> getDataNonBlocking(String tenantId, FrontEndQuery frontEndQuery, DataCollection.Version version);

}
