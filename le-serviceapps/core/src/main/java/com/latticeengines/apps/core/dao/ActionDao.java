package com.latticeengines.apps.core.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Action;

public interface ActionDao extends BaseDao<Action> {

    List<Action> findAllWithNullOwnerId();

    void updateOwnerIdIn(Long ownerId, List<Long> actionPids);

    void cancel(Long actionPid);

}
