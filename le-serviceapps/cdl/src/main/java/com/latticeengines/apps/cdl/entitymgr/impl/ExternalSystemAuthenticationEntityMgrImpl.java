package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.ExternalSystemAuthenticationDao;
import com.latticeengines.apps.cdl.entitymgr.ExternalSystemAuthenticationEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.repository.ExternalSystemAuthenticationRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("externalSystemAuthenticationEntityMgr")
public class ExternalSystemAuthenticationEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<ExternalSystemAuthenticationRepository, ExternalSystemAuthentication, Long>
        implements ExternalSystemAuthenticationEntityMgr {

    @Inject
    private ExternalSystemAuthenticationEntityMgrImpl _self;
    
    @Inject
    private ExternalSystemAuthenticationDao extSysAuthenticationEntityMgrDao;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Resource(name = "externalSystemAuthenticationWriterRepository")
    private ExternalSystemAuthenticationRepository extSysAuthenticationWriterRepository;

    @Resource(name = "externalSystemAuthenticationReaderRepository")
    private ExternalSystemAuthenticationRepository extSysAuthenticationReaderRepository;
    
    @Override
    public BaseDao<ExternalSystemAuthentication> getDao() {
        return extSysAuthenticationEntityMgrDao;
    }

    @Override
    protected ExternalSystemAuthenticationRepository getReaderRepo() {
        return extSysAuthenticationReaderRepository;
    }

    @Override
    protected ExternalSystemAuthenticationRepository getWriterRepo() {
        return extSysAuthenticationWriterRepository;
    }

    @Override
    protected ExternalSystemAuthenticationEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ExternalSystemAuthentication createAuthentication(
            ExternalSystemAuthentication externalSystemAuthentication) {
        if (StringUtils.isBlank(externalSystemAuthentication.getLookupMapConfigId())) {
            throw new LedpException(LedpCode.LEDP_40049);
        }
        LookupIdMap lookupIdRef = lookupIdMappingEntityMgr
                .getLookupIdMap(externalSystemAuthentication.getLookupMapConfigId());
        if (lookupIdRef == null) {
            throw new LedpException(LedpCode.LEDP_40050, new String[] {externalSystemAuthentication.getLookupMapConfigId()});
        }
        Tenant tenant = MultiTenantContext.getTenant();
        externalSystemAuthentication.setTenant(tenant);
        externalSystemAuthentication.setLookupIdMap(lookupIdRef);
        externalSystemAuthentication.setLookupMapConfigId(lookupIdRef.getId());
        externalSystemAuthentication.setId(UUID.randomUUID().toString());
        Date time = new Date();
        externalSystemAuthentication.setCreated(time);
        externalSystemAuthentication.setUpdated(time);
        getDao().create(externalSystemAuthentication);
        return externalSystemAuthentication;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ExternalSystemAuthentication updateAuthentication(String authId,
            ExternalSystemAuthentication externalSystemAuthentication) {
            if (StringUtils.isBlank(authId)) {
                throw new LedpException(LedpCode.LEDP_40051);
            }

            ExternalSystemAuthentication extSysAuth = getWriterRepo().findByAuthId(authId);
            if (extSysAuth == null) {
                throw new LedpException(LedpCode.LEDP_40052, new String[] {authId});
            }
            // This will expand the Lazy Proxy for LookupIdMap. 
            // This is needed because we are loading Object from WriterRepository and Saving it via Dao. Both have different SessionContexts
            extSysAuth.getLookupIdMap().getId();
            extSysAuth.getTenant().getId();

            extSysAuth.setTrayAuthenticationId(externalSystemAuthentication.getTrayAuthenticationId());
            extSysAuth.setUpdated(new Date());
            getDao().update(extSysAuth);
            return extSysAuth;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly=true)
    public List<ExternalSystemAuthentication> findAuthenticationsByLookupMapIds(List<String> lookupMapIds) {
        return getReaderRepo().findByLookupMapIds(lookupMapIds);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly=true)
    public ExternalSystemAuthentication findAuthenticationByAuthId(String authId) {
        return getReaderRepo().findByAuthId(authId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly=true)
    public List<ExternalSystemAuthentication> findAuthentications() {
        return getReaderRepo().findAllAuths();
    }

}
