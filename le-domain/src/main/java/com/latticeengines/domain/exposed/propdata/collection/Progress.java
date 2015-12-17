package com.latticeengines.domain.exposed.propdata.collection;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

public interface Progress extends HasPid{

    ProgressStatus getStatus();

    void setStatus(ProgressStatus status);

    void setStatusBeforeFailed(ProgressStatus status);

    ProgressStatus getStatusBeforeFailed();

    void setErrorMessage(String errorMessage);

    String getSourceName();

    String getRootOperationUID();

    int getNumRetries();

    void setNumRetries(int numRetries);

}
