package com.latticeengines.propdata.collection.service;

import java.util.Date;

import com.latticeengines.propdata.collection.util.DateRange;

public interface ArchiveService {

    CollectionJobContext startNewProgress(Date startDate, Date endDate, String creator);

    CollectionJobContext importFromDB(CollectionJobContext request);

    CollectionJobContext transformRawData(CollectionJobContext request);

    CollectionJobContext exportToDB(CollectionJobContext request);

    CollectionJobContext findJobToRetry();

    CollectionJobContext findRunningJob();

    DateRange determineNewJobDateRange();

}
