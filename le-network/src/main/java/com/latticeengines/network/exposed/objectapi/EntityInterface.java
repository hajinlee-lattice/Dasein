package com.latticeengines.network.exposed.objectapi;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;

public interface EntityInterface {
    long getCount(String customerSpace, Query query);

    DataPage getData(String customerSpace, Query query);
}
