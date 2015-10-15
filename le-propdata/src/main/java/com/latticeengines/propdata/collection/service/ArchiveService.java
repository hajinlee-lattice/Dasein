package com.latticeengines.propdata.collection.service;

import java.util.Date;

public interface ArchiveService {

    CollectionJobContext startNewProgress(Date startDate, Date endDate, String creator);

    CollectionJobContext importFromDB(CollectionJobContext request);

    CollectionJobContext transformRawData(CollectionJobContext request);

    CollectionJobContext exportToDB(CollectionJobContext request);

}
