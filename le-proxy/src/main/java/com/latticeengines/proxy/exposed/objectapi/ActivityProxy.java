package com.latticeengines.proxy.exposed.objectapi;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.ActivityTimelineQuery;
import com.latticeengines.domain.exposed.query.DataPage;

public interface ActivityProxy {

    DataPage getData(String customerSpace, DataCollection.Version version, ActivityTimelineQuery activityTimelineQuery);
}
