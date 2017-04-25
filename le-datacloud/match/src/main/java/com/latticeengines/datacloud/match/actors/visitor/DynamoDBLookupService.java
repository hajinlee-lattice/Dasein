package com.latticeengines.datacloud.match.actors.visitor;

import java.util.Map;

public interface DynamoDBLookupService {
    Map<String, Integer> getPendingReqStats();
}
