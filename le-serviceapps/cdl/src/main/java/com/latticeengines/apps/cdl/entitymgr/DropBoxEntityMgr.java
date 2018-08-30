package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.DropBox;

public interface DropBoxEntityMgr extends BaseEntityMgrRepository<DropBox, Long> {

    DropBox createDropBox();

    DropBox getDropBox();

}
