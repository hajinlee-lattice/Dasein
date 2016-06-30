package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.metadata.dao.DataRuleDao;

@Component("dataRuleDao")
public class DataRuleDaoImpl extends BaseDaoImpl<DataRule> implements DataRuleDao {

    @Override
    protected Class<DataRule> getEntityClass() {
        return DataRule.class;
    }

}
