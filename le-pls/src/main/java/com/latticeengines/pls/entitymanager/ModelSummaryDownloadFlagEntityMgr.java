package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;

public interface ModelSummaryDownloadFlagEntityMgr extends BaseEntityMgr<ModelSummaryDownloadFlag> {

    List<String> getWaitingFlags();

    void addDownloadFlag(String tenantId);

    void removeDownloadedFlag(long timeTicks);
}
