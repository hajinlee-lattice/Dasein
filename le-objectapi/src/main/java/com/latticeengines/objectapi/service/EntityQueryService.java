package com.latticeengines.objectapi.service;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;

import reactor.core.publisher.Flux;

public interface EntityQueryService {

    long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version);

    Flux<Map<String, Object>> getDataFlux(FrontEndQuery frontEndQuery, DataCollection.Version version);

    Map<String, Long> getRatingCount(RatingEngineFrontEndQuery frontEndQuery, DataCollection.Version version);

}
