package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.dao.SourceAttributeDao;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;

@Component("sourceAttributeEntityMgr")
public class SourceAttributeEntityMgrImpl implements SourceAttributeEntityMgr {

    @Autowired
    private SourceAttributeDao sourceAttributeDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public void createAttribute(SourceAttribute attr) {
        sourceAttributeDao.create(attr);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<SourceAttribute> getAttributes(String source, String stage, String transform) {
        return sourceAttributeDao.getAttributes(source, stage, transform);
    }
}
