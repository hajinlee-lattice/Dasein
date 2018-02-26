package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.entitymgr.AttributeEntityMgr;

@Component("attributeEntityMgr")
public class AttributeEntityMgrImpl extends BaseEntityMgrImpl<Attribute> implements AttributeEntityMgr {

    @Autowired
    private AttributeDao attributeDao;

    @Override
    public BaseDao<Attribute> getDao() {
        return attributeDao;
    }

}
