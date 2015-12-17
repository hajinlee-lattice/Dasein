package com.latticeengines.propdata.collection.entitymanager;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressStatus;
import com.latticeengines.propdata.collection.source.Source;

public interface ArchiveProgressEntityMgr {

    ArchiveProgress createProgress(ArchiveProgress progress);

    ArchiveProgress updateProgress(ArchiveProgress progress);

    ArchiveProgress updateStatus(ArchiveProgress progress, ArchiveProgressStatus status);

    void deleteProgressByRootOperationUid(String rootOperationUid);

    ArchiveProgress findProgressByRootOperationUid(String rootOperationUid);

    ArchiveProgress insertNewProgress(Source source, Date startDate, Date endDate, String creator);

    ArchiveProgress findEarliestFailureUnderMaxRetry(Source source);

    ArchiveProgress findProgressNotInFinalState(Source source);

}
