package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.pls.dao.RuleBasedModelDao;

@Component("ruleBasedModelDao")
public class RuleBasedModelDaoImpl extends BaseDaoImpl<RuleBasedModel> implements RuleBasedModelDao {

    @Override
    protected Class<RuleBasedModel> getEntityClass() {
        return RuleBasedModel.class;
    }

    @Override
    public RuleBasedModel findById(String id) {
        return super.findByField("ID", id);
    }
}
