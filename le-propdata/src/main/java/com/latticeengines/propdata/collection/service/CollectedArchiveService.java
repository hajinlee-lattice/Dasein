package com.latticeengines.propdata.collection.service;

import com.latticeengines.propdata.collection.util.DateRange;

public interface CollectedArchiveService extends ArchiveService {

    DateRange determineNewJobDateRange();
}
