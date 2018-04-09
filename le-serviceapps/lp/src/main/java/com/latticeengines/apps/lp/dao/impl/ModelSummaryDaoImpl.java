package com.latticeengines.apps.lp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.ModelSummaryDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummary;

@Component("modelSummaryDao")
public class ModelSummaryDaoImpl extends BaseDaoImpl<ModelSummary> implements ModelSummaryDao {

    @Override
    protected Class<ModelSummary> getEntityClass() {
        return ModelSummary.class;
    }

}
