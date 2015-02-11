package com.latticeengines.pls.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.pls.dao.PredictorElementDao;

public class PredictorElementDaoImpl extends BaseDaoImpl<PredictorElement> implements PredictorElementDao {

    @Override
    protected Class<PredictorElement> getEntityClass() {
        return PredictorElement.class;
    }

}
