package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.DnBMatchOutput;

public interface DnBRealTimeLookupService {

    DnBMatchOutput realtimeEntityLookup(MatchKeyTuple input);

    DnBMatchOutput realtimeEmailLookup(MatchKeyTuple input);
}
