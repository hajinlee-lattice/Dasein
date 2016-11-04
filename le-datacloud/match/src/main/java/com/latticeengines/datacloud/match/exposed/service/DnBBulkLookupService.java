package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBBulkMatchInfo;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;

public interface DnBBulkLookupService {
    public DnBBulkMatchInfo sendRequest(List<MatchKeyTuple> input);

    public List<DnBMatchOutput> getResult(DnBBulkMatchInfo info);
}
