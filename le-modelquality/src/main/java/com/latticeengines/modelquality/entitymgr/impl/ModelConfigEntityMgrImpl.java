package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.ModelConfig;
import com.latticeengines.modelquality.dao.ModelConfigDao;
import com.latticeengines.modelquality.entitymgr.ModelConfigEntityMgr;

/**
 * This entity manager is responsible for persisting predefined or template
 * config.
 */
@Component("modelConfigEntityMgr")
public class ModelConfigEntityMgrImpl extends BaseEntityMgrImpl<ModelConfig> implements ModelConfigEntityMgr {

    @Autowired
    private ModelConfigDao modelConfigDao;

    @Override
    public BaseDao<ModelConfig> getDao() {
        return modelConfigDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createModelConfigs(List<ModelConfig> modelConfigs) {
        for (ModelConfig modelConfig : modelConfigs) {
            modelConfigDao.create(modelConfig);
        }
    }

}
