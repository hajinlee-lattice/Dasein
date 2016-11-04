package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.DnBBulkMatchInfo;
import com.latticeengines.domain.exposed.datacloud.match.DnBMatchOutput;

public interface DnBBulkLookupService {
    public DnBBulkMatchInfo sendRequest(List<MatchKeyTuple> input);

    public List<DnBMatchOutput> getResult(DnBBulkMatchInfo info);
}
