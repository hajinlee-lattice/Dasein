package com.latticeengines.datacloud.match.dao.impl;

import com.latticeengines.datacloud.match.dao.DecisionGraphDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

public class DecisionGraphDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<DecisionGraph> implements DecisionGraphDao {

    public DecisionGraphDaoImpl() { super(); }

    @Override
    protected Class<DecisionGraph> getEntityClass() {
        return DecisionGraph.class;
    }

}
