package com.latticeengines.modelquality.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.modelquality.dao.ModelRunDao;

@Component("qualityModelRunDao")
public class ModelRunDaoImpl extends BaseDaoImpl<ModelRun> implements ModelRunDao {

    @Override
    protected Class<ModelRun> getEntityClass() {
        return ModelRun.class;
    }

}
