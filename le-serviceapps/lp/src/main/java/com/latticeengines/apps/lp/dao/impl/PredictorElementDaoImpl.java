package com.latticeengines.apps.lp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.PredictorElementDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.PredictorElement;

/**
 * Change to this dao should also be made to le-pls
 */
@Component("predictorElementDao")
public class PredictorElementDaoImpl extends BaseDaoImpl<PredictorElement> implements PredictorElementDao {

    @Override
    protected Class<PredictorElement> getEntityClass() {
        return PredictorElement.class;
    }

}
