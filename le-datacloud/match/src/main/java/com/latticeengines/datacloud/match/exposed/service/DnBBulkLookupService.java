package com.latticeengines.datacloud.match.exposed.service;

import java.util.Map;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBulkMatchInfo;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;

public interface DnBBulkLookupService {
    public DnBBulkMatchInfo sendRequest(Map<String, MatchKeyTuple> input);

    public Map<String, DnBMatchOutput> getResult(DnBBulkMatchInfo info);
}
