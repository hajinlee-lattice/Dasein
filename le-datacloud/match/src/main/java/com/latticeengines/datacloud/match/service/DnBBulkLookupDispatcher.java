package com.latticeengines.datacloud.match.service;

import java.util.Map;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;

public interface DnBBulkLookupDispatcher {
    public DnBBulkMatchInfo sendRequest(Map<String, MatchKeyTuple> input);
}
