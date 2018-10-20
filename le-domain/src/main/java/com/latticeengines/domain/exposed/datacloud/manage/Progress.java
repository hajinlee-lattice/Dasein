package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.Date;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

public interface Progress extends HasPid {

    ProgressStatus getStatus();

    void setStatus(ProgressStatus status);

    ProgressStatus getStatusBeforeFailed();

    void setStatusBeforeFailed(ProgressStatus status);

    void setErrorMessage(String errorMessage);

    String getSourceName();

    String getRootOperationUID();

    int getNumRetries();

    void setNumRetries(int numRetries);

    Date getCreateTime();

    Date getLatestStatusUpdate();

}
