package com.latticeengines.objectapi.service;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;

public interface EntityQueryService {

    long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser);

    DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser,
            boolean enforceTranslation);

    Map<String, Long> getRatingCount(RatingEngineFrontEndQuery frontEndQuery, DataCollection.Version version,
            String sqlUser);

    String getQueryStr(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser);

}
