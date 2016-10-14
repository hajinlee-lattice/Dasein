package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.dao.PropDataDao;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;

@Component("propDataEntityMgr")
public class PropDataEntityMgrImpl extends BaseEntityMgrImpl<PropData> implements PropDataEntityMgr {

    @Autowired
    private PropDataDao propDataDao;

    @Override
    public BaseDao<PropData> getDao() {
        return propDataDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(PropData propdata) {
        propdata.setName(propdata.getName().replace('/', '_'));
        propDataDao.create(propdata);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createPropDatas(List<PropData> propDatas) {
        for (PropData propData : propDatas) {
            propData.setName(propData.getName().replace('/', '_'));
            propDataDao.create(propData);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PropData findByName(String propDataConfigName) {
        return propDataDao.findByField("NAME", propDataConfigName);
    }

}
