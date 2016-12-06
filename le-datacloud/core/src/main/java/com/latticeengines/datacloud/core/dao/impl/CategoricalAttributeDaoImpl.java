package com.latticeengines.datacloud.core.dao.impl;

import com.latticeengines.datacloud.core.dao.CategoricalAttributeDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;

public class CategoricalAttributeDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CategoricalAttribute>
        implements CategoricalAttributeDao {

    @Override
    protected Class<CategoricalAttribute> getEntityClass() {
        return CategoricalAttribute.class;
    }

}
