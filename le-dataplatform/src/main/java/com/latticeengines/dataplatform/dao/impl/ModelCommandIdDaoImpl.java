package com.latticeengines.dataplatform.dao.impl;

import com.latticeengines.dataplatform.dao.ModelCommandIdDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandId;

public class ModelCommandIdDaoImpl extends BaseDaoImpl<ModelCommandId> implements ModelCommandIdDao {

    public ModelCommandIdDaoImpl() {
        super();
    }

    @Override
    protected Class<ModelCommandId> getEntityClass() {
        return ModelCommandId.class;
    }

}
