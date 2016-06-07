package com.latticeengines.quartzclient.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.quartz.ActiveStack;

public interface ActiveStackEntityMgr extends BaseEntityMgr<ActiveStack> {

    String getActiveStack();

}
