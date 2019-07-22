package com.latticeengines.objectapi.service;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public interface EventQueryService {
    long getScoringCount(EventFrontEndQuery frontEndQuery, DataCollection.Version version);

    long getTrainingCount(EventFrontEndQuery frontEndQuery, DataCollection.Version version);

    long getEventCount(EventFrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getScoringTuples(EventFrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getTrainingTuples(EventFrontEndQuery frontEndQuery, DataCollection.Version version);

    DataPage getEventTuples(EventFrontEndQuery frontEndQuery, DataCollection.Version version);

    String getQueryStr(EventFrontEndQuery frontEndQuery, EventType eventType, Version version);

    Map<String, String> getSplitedSparkSQLQueries(EventFrontEndQuery frontEndQuery, EventType eventType, //
                                                   DataCollection.Version version);
}
