package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.domain.exposed.datacloud.match.DnBMatchEntry;

import java.util.List;

public interface DnBBulkLookupService {
    public List<DnBMatchEntry> bulkEntitiesLookup(List<DnBMatchEntry> input);
    public List<DnBMatchEntry> bulkEmailsLookup(List<DnBMatchEntry> input);
}
