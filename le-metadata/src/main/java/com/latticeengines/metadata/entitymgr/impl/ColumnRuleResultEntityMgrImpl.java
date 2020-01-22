package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.metadata.dao.ColumnRuleResultDao;
import com.latticeengines.metadata.entitymgr.ColumnRuleResultEntityMgr;

@Component("columnRuleResultEntityMgr")
public class ColumnRuleResultEntityMgrImpl extends BaseEntityMgrImpl<ColumnRuleResult> implements ColumnRuleResultEntityMgr {

    @Inject
    private ColumnRuleResultDao columnRuleResultDao;

    @Override
    public BaseDao<ColumnRuleResult> getDao() {
        return columnRuleResultDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<ColumnRuleResult> findByModelId(String modelId) {
        return columnRuleResultDao.findByModelId(modelId);
    }

}
