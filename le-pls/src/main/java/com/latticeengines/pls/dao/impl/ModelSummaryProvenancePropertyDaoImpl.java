package com.latticeengines.pls.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenanceProperty;
import com.latticeengines.pls.dao.ModelSummaryProvenancePropertyDao;
import org.springframework.stereotype.Component;

@Component("modelSummaryProvenancePropertyDao")
public class ModelSummaryProvenancePropertyDaoImpl extends BaseDaoImpl<ModelSummaryProvenanceProperty> implements
        ModelSummaryProvenancePropertyDao {
    @Override
    protected Class<ModelSummaryProvenanceProperty> getEntityClass() {
        return ModelSummaryProvenanceProperty.class;
    }
}
