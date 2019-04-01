package com.latticeengines.objectapi.service;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.spark.LivySession;

public interface RatingQueryService {

    long getCount(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser);

    DataPage getData(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser);

    Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser);

    String getQueryStr(FrontEndQuery frontEndQuery, DataCollection.Version version, String sqlUser);

    HdfsDataUnit getDataViaSparkSQL(FrontEndQuery frontEndQuery, DataCollection.Version version,
                                    LivySession livySession);

}
