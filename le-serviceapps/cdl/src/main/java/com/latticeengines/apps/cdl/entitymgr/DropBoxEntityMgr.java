package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DropBoxEntityMgr extends BaseEntityMgrRepository<DropBox, Long> {

    DropBox createDropBox(String region);

    DropBox getDropBox();

    Tenant getDropBoxOwner(String dropBox);

}
