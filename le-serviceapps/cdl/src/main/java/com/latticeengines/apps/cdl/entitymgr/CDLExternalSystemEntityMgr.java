package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;

public interface CDLExternalSystemEntityMgr extends BaseEntityMgr<CDLExternalSystem> {

    List<CDLExternalSystem> findAllExternalSystem();
}
