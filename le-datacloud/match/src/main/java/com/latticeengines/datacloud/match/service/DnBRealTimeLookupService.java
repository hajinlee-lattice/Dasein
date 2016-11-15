package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.dnb.DnBMatchContext;

public interface DnBRealTimeLookupService {

    DnBMatchContext realtimeEntityLookup(DnBMatchContext input);

    DnBMatchContext realtimeEmailLookup(DnBMatchContext input);
}
