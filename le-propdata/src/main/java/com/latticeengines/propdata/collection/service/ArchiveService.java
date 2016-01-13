package com.latticeengines.propdata.collection.service;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.core.source.RawSource;

public interface ArchiveService {

    ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator);

    ArchiveProgress importFromDB(ArchiveProgress request);

    ArchiveProgress finish(ArchiveProgress progress);

    String getVersionString(ArchiveProgress progress);

    RawSource getSource();

}
