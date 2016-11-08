package com.latticeengines.datacloud.match.exposed.service;

import java.util.Map;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBBulkMatchInfo;
import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;

public interface DnBBulkLookupService {
    public DnBBulkMatchInfo sendRequest(Map<String, MatchKeyTuple> input);

    public Map<String, DnBMatchOutput> getResult(DnBBulkMatchInfo info);
}
