package com.latticeengines.metadata.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.metadata.dao.AttributeDao;

@Component("attributeDao")
public class AttributeDaoImpl extends BaseDaoImpl<Attribute> implements AttributeDao {

    @Override
    protected Class<Attribute> getEntityClass() {
        return Attribute.class;
    }

}
