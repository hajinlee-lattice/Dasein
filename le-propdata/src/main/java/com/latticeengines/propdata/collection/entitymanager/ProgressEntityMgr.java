package com.latticeengines.propdata.collection.entitymanager;

import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.source.Source;

public interface ProgressEntityMgr<P> {

    P updateProgress(P progress);

    P updateStatus(P progress, ProgressStatus status);

    void deleteProgressByRootOperationUid(String rootOperationUid);

    P findProgressByRootOperationUid(String rootOperationUid);

    P findEarliestFailureUnderMaxRetry(Source source);

    P findRunningProgress(Source source);

    void deleteAllProgressesOfSource(Source source);

}
