package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.match.DnBMatchEntry;

public interface DnBLookupService {
    public DnBMatchEntry realtimeEntityLookup(DnBMatchEntry input);

    public DnBMatchEntry realtimeEmailLookup(DnBMatchEntry input);
}
