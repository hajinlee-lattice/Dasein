package com.latticeengines.apps.lp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.ModelSummaryProvenancePropertyDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenanceProperty;

/**
 * Change to this dao should also be made to le-pls
 */
@Component("modelSummaryProvenancePropertyDao")
public class ModelSummaryProvenancePropertyDaoImpl extends BaseDaoImpl<ModelSummaryProvenanceProperty> implements
        ModelSummaryProvenancePropertyDao {
    @Override
    protected Class<ModelSummaryProvenanceProperty> getEntityClass() {
        return ModelSummaryProvenanceProperty.class;
    }
}
