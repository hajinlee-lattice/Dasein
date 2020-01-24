package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.metadata.dao.RowRuleResultDao;
import com.latticeengines.metadata.entitymgr.RowRuleResultEntityMgr;

@Component("rowRuleResultEntityMgr")
public class RowRuleResultEntityMgrImpl extends BaseEntityMgrImpl<RowRuleResult> implements RowRuleResultEntityMgr {

    @Inject
    private RowRuleResultDao rowRuleResultDao;

    @Override
    public BaseDao<RowRuleResult> getDao() {
        return rowRuleResultDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<RowRuleResult> findByModelId(String modelId) {
        return rowRuleResultDao.findByModelId(modelId);
    }

}
