package com.latticeengines.propdata.core.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.ExternalColumn;
import com.latticeengines.propdata.core.dao.ExternalColumnDao;
import com.latticeengines.propdata.core.entitymgr.ExternalColumnEntityMgr;

@Component("externalColumnEntityMgr")
public class ExternalColumnEntityMgrImpl implements ExternalColumnEntityMgr {

    @Autowired
    private ExternalColumnDao externalColumnDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> getExternalColumns() {
        return externalColumnDao.findAll();
    }

}
