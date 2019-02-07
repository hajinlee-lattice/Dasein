package com.latticeengines.apps.cdl.dao.impl;

import java.util.Date;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ExternalSystemAuthenticationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("externalSystemAuthenticationDao")
public class ExternalSystemAuthenticationDaoImpl extends BaseDaoImpl<ExternalSystemAuthentication> implements ExternalSystemAuthenticationDao {

    @Override
    protected Class<ExternalSystemAuthentication> getEntityClass() {
        return ExternalSystemAuthentication.class;
    }

    /*
     * Need to provide the implementation at DAO layer. 
     * So that, it can be invoked directly from ExternalSystemAuthentication resource or LookupIdMap resource
     */
    @Override
    public void create(ExternalSystemAuthentication externalSystemAuthentication) {
        if (externalSystemAuthentication.getLookupIdMap() == null || externalSystemAuthentication.getLookupIdMap().getPid() == null) {
            throw new LedpException(LedpCode.LEDP_40049);
        }

        Tenant tenant = MultiTenantContext.getTenant();
        externalSystemAuthentication.setTenant(tenant);
        externalSystemAuthentication.setLookupMapConfigId(externalSystemAuthentication.getLookupIdMap().getId());
        externalSystemAuthentication.setId(UUID.randomUUID().toString());
        Date time = new Date();
        externalSystemAuthentication.setCreated(time);
        externalSystemAuthentication.setUpdated(time);
        super.create(externalSystemAuthentication);
    }

    @Override
    public ExternalSystemAuthentication updateAuthentication(ExternalSystemAuthentication extSysAuth) {
        ExternalSystemAuthentication extSysAuthFromDB = super.findByField("Id", extSysAuth.getId());
        if (extSysAuthFromDB == null) {
            throw new LedpException(LedpCode.LEDP_40052, new String[] {extSysAuth.getId()});
        }

        extSysAuthFromDB.setTrayAuthenticationId(extSysAuth.getTrayAuthenticationId());
        extSysAuthFromDB.setSolutionInstanceId(extSysAuth.getSolutionInstanceId());
        extSysAuthFromDB.setTrayWorkflowEnabled(extSysAuth.getTrayWorkflowEnabled());
        extSysAuthFromDB.setUpdated(new Date());
        super.update(extSysAuthFromDB);

        if (extSysAuthFromDB.getLookupIdMap() != null) {
            extSysAuthFromDB.setLookupMapConfigId(extSysAuthFromDB.getLookupIdMap().getId());
        }
        return extSysAuthFromDB;
    }

}
