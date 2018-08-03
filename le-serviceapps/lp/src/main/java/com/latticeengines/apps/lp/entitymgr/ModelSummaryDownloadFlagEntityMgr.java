package com.latticeengines.apps.lp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;

public interface ModelSummaryDownloadFlagEntityMgr extends BaseEntityMgr<ModelSummaryDownloadFlag> {

    List<String> getWaitingFlags();

    List<String> getExcludeFlags();

    void addDownloadFlag(String tenantId);

    void addExcludeFlag(String tenantId);

    void removeDownloadedFlag(long timeTicks);

    void removeExcludeFlag(String tenantId);
}
