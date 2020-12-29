package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;

public interface ActivityTimelineQueryService {
    DataPage getData(String customerSpace, DataCollection.Version version, ActivityTimelineQuery activityTimelineQuery);
}
