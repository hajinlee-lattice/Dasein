package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.DecisionGraphDao;
import com.latticeengines.datacloud.match.entitymgr.DecisionGraphEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

@Component("decisionGraphyEntityMgr")
public class DecisionGraphEntityMgrImpl implements DecisionGraphEntityMgr {

    @Autowired
    private DecisionGraphDao decisionGraphDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DecisionGraph getDecisionGraph(String graphName) {
        return decisionGraphDao.findByField("graphName", graphName);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DecisionGraph> findAll() {
        return decisionGraphDao.findAll();
    }

}
