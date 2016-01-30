package com.latticeengines.propdata.match.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.dao.ExternalColumnDao;
import com.latticeengines.propdata.match.entitymanager.ExternalColumnEntityMgr;

@Component("externalColumnEntityMgr")
public class ExternalColumnEntityMgrImpl implements ExternalColumnEntityMgr {

    @Autowired
    private ExternalColumnDao externalColumnDao;
    
    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> getLeadEnrichment() {
        return externalColumnDao.getLeadEnrichment();
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> findByTag(String tag) {
        return externalColumnDao.findByTag(tag);
    }

}
