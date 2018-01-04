package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.modelquality.dao.ModelConfigDao;

@Component("modelConfigDao")
public class ModelConfigDaoImpl extends ModelQualityBaseDaoImpl<ModelConfig> implements ModelConfigDao {

    @Override
    protected Class<ModelConfig> getEntityClass() {
        return ModelConfig.class;
    }

}
