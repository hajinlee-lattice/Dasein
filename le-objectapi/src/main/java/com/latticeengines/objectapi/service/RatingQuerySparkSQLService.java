package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface RatingQuerySparkSQLService extends RatingQueryService {

    HdfsDataUnit getRatingData(FrontEndQuery frontEndQuery, DataCollection.Version version);

}
