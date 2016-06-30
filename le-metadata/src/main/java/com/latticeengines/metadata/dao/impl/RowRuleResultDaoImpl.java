package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelreview.RowRuleResult;
import com.latticeengines.metadata.dao.RowRuleResultDao;

@Component("rowRuleResultDao")
public class RowRuleResultDaoImpl extends BaseDaoImpl<RowRuleResult> implements RowRuleResultDao {

    @Override
    protected Class<RowRuleResult> getEntityClass() {
        return RowRuleResult.class;
    }

}
