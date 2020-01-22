package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.CDLExternalSystemDao;
import com.latticeengines.apps.cdl.entitymgr.CDLExternalSystemEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("cdlExternalSystemEntityMgr")
public class CDLExternalSystemEntityMgrImpl extends BaseEntityMgrImpl<CDLExternalSystem>
        implements CDLExternalSystemEntityMgr {

    @Inject
    private CDLExternalSystemDao cdlExternalSystemDao;

    @Override
    public BaseDao<CDLExternalSystem> getDao() {
        return cdlExternalSystemDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<CDLExternalSystem> findAllExternalSystem() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public CDLExternalSystem findExternalSystem(BusinessEntity entity) {
        CDLExternalSystem externalSystem = super.findByField("ENTITY", entity.name());
        if (externalSystem == null) {
            return null;
        } else {
            return externalSystem;
        }
    }
}
