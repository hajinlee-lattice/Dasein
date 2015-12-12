package com.latticeengines.pls.entitymanager.impl;

import java.util.Map;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.AmAttribute;
import com.latticeengines.pls.dao.AmAttributeDao;
import com.latticeengines.pls.entitymanager.AmAttributeEntityMgr;

@Component("amAttributeEntityMgr")
public class AmAttributeEntityMgrImpl extends BaseEntityMgrImpl<AmAttribute> implements AmAttributeEntityMgr {

    @Autowired
    private AmAttributeDao amAttributeDao;

    @Override
    public BaseDao<AmAttribute> getDao() {
        return amAttributeDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AmAttribute> findAttributes(String key, String parentKey, String parentValue) {
        return amAttributeDao.findAttributes(key, parentKey, parentValue);
    }
}

