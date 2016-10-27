package com.latticeengines.datacloud.collection.service;

import com.latticeengines.datacloud.core.util.DateRange;

public interface CollectedArchiveService extends ArchiveService {

    DateRange determineNewJobDateRange();
}
