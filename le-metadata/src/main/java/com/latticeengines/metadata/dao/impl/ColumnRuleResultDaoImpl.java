package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelreview.ColumnRuleResult;
import com.latticeengines.metadata.dao.ColumnRuleResultDao;

@Component("columnRuleResultDao")
public class ColumnRuleResultDaoImpl extends BaseDaoImpl<ColumnRuleResult> implements ColumnRuleResultDao {

    @Override
    protected Class<ColumnRuleResult> getEntityClass() {
        return ColumnRuleResult.class;
    }

}
