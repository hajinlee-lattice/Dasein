package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.pls.dao.ActionDao;

@Component("actionDao")
public class ActionDaoImpl extends BaseDaoImpl<Action> implements ActionDao {

    @Override
    protected Class<Action> getEntityClass() {
        return Action.class;
    }

}
