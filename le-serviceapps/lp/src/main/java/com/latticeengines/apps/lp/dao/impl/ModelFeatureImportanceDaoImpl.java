package com.latticeengines.apps.lp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.ModelFeatureImportanceDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;

@Component("modelFeatureImportanceDao")
public class ModelFeatureImportanceDaoImpl extends BaseDaoImpl<ModelFeatureImportance>
        implements ModelFeatureImportanceDao {

    @Override
    protected Class<ModelFeatureImportance> getEntityClass() {
        return ModelFeatureImportance.class;
    }

}
