package com.latticeengines.quartzclient.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.quartz.ActiveStack;

public interface ActiveStackDao extends BaseDao<ActiveStack> {

    String getActiveStack();

}
