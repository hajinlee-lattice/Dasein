package com.latticeengines.network.exposed.objectapi;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.query.Query;

public interface AccountInterface {

    long getCount(Query query);

    double getLift(Query query);

    List<Map<String, Object>> getData(Query query);
}
