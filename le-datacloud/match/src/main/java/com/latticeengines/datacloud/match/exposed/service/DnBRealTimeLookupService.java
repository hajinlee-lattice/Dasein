package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.domain.exposed.datacloud.match.DnBMatchEntry;

public interface DnBRealTimeLookupService {

    public DnBMatchEntry realtimeEntityLookup(DnBMatchEntry input);

    public DnBMatchEntry realtimeEmailLookup(DnBMatchEntry input);
}
