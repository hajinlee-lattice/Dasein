package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public interface EventQueryService {
    long getScoringCount(EventFrontEndQuery frontEndQuery, String sqlUser, DataCollection.Version version);

    long getTrainingCount(EventFrontEndQuery frontEndQuery, String sqlUser, DataCollection.Version version);

    long getEventCount(EventFrontEndQuery frontEndQuery, String sqlUser, DataCollection.Version version);

    DataPage getScoringTuples(EventFrontEndQuery frontEndQuery, String sqlUser, DataCollection.Version version);

    DataPage getTrainingTuples(EventFrontEndQuery frontEndQuery, String sqlUser, DataCollection.Version version);

    DataPage getEventTuples(EventFrontEndQuery frontEndQuery, String sqlUser, DataCollection.Version version);

    String getQueryStr(EventFrontEndQuery frontEndQuery, EventType eventType, String sqlUser, Version version);
}
