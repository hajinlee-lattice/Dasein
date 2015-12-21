package com.latticeengines.propdata.collection.service;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.util.DateRange;

public interface ArchiveService {

    ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator);

    ArchiveProgress importFromDB(ArchiveProgress request);

    ArchiveProgress transformRawData(ArchiveProgress progress);

    ArchiveProgress exportToDB(ArchiveProgress progress);

    ArchiveProgress finish(ArchiveProgress progress);

    ArchiveProgress findJobToRetry();

    ArchiveProgress findRunningJob();

    DateRange determineNewJobDateRange();

}
