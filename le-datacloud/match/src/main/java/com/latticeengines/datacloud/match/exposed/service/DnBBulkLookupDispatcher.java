package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;

import java.util.Map;

public interface DnBBulkLookupDispatcher {
    public DnBBulkMatchInfo sendRequest(Map<String, MatchKeyTuple> input);
}
