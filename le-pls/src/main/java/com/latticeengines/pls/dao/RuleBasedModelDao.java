package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;

public interface RuleBasedModelDao extends BaseDao<RuleBasedModel> {

    RuleBasedModel findById(String id);

}
