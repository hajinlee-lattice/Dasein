package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;

public interface DnBRealTimeLookupService {

    DnBMatchContext realtimeEntityLookup(MatchKeyTuple input);

    DnBMatchContext realtimeEmailLookup(MatchKeyTuple input);
}
