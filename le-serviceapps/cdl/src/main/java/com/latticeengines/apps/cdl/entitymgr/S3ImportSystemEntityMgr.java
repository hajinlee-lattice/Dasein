package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

public interface S3ImportSystemEntityMgr extends BaseEntityMgrRepository<S3ImportSystem, Long> {

    void createS3ImportSystem(S3ImportSystem importSystem);

    S3ImportSystem findS3ImportSystem(String name);
}
