package com.latticeengines.dataplatform.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;

public interface ModelDownloadFlagEntityMgr extends BaseEntityMgr<ModelSummaryDownloadFlag> {

    void addDownloadFlag(String tenantId);
}
