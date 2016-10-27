package com.latticeengines.datacloud.collection.entitymgr;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

public interface ProgressEntityMgr<P> {

    P updateProgress(P progress);

    P updateStatus(P progress, ProgressStatus status);

    void deleteProgressByRootOperationUid(String rootOperationUid);

    P findProgressByRootOperationUid(String rootOperationUid);

    P findEarliestFailureUnderMaxRetry(Source source);

    P findRunningProgress(Source source);

    void deleteAllProgressesOfSource(Source source);

}
