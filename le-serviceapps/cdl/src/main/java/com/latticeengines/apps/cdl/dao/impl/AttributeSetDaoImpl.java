package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AttributeSetDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.AttributeSet;

@Component("attributeSetDao")
public class AttributeSetDaoImpl extends BaseDaoImpl<AttributeSet> implements AttributeSetDao {
    @Override
    protected Class<AttributeSet> getEntityClass() {
        return AttributeSet.class;
    }
}
