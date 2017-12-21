package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Action;

public interface ActionDao extends BaseDao<Action> {

    List<Action> findAllWithNullOwnerId();

}
