package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;

import java.util.List;

public interface ModelSummaryDownloadFlagEntityMgr extends BaseEntityMgr<ModelSummaryDownloadFlag> {

    List<ModelSummaryDownloadFlag> getWaitingFlags();

    void addDownloadFlag(String tenantId);

    void removeDownloadedFlag(long timeTicks);
}
