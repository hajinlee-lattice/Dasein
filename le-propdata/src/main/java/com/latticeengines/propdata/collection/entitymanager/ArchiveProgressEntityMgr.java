package com.latticeengines.propdata.collection.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressStatus;

public interface ArchiveProgressEntityMgr<T> {

    T insertNewProgress(Date startDate, Date endDate, String creator) throws InstantiationException, IllegalAccessException;

    T updateProgress(T progress);

    T updateStatus(T progress, ArchiveProgressStatus status);

    void deleteProgressByRootOperationUid(String rootOperationUid);

    T findProgressByRootOperationUid(String rootOperationUid);

    Class<T> getProgressClass();

    T findEarliestFailureUnderMaxRetry();

    T findProgressNotInFinalState();

}
