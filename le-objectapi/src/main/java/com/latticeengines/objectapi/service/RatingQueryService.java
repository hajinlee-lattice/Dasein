package com.latticeengines.objectapi.service;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface RatingQueryService {

    long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version);

    Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery, DataCollection.Version version);

}
