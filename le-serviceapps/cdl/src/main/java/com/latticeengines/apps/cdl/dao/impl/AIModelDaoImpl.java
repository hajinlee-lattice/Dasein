package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.AIModel;

@Component("aiModelDao")
public class AIModelDaoImpl extends BaseDaoImpl<AIModel> implements AIModelDao {

	@Override
    protected Class<AIModel> getEntityClass() {
        return AIModel.class;
    }

}
