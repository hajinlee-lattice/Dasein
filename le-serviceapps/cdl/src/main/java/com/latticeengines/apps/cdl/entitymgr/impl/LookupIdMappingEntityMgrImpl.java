package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataMappingDao;
import com.latticeengines.apps.cdl.dao.ExternalSystemAuthenticationDao;
import com.latticeengines.apps.cdl.dao.LookupIdMappingDao;
import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.LookupIdMappingRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("lookupIdMappingEntityMgr")
public class LookupIdMappingEntityMgrImpl extends BaseEntityMgrRepositoryImpl<LookupIdMap, Long>
        implements LookupIdMappingEntityMgr {

    @Inject
    private LookupIdMappingDao lookupIdMappingEntityMgrDao;

    @Inject
    private LookupIdMappingRepository lookupIdMappingRepository;

    @Inject
    private ExternalSystemAuthenticationDao extSysAuthenticationDao;

    @Inject
    private ExportFieldMetadataMappingDao exportFieldMetadataMappingDao;

    @Inject
    private ExportFieldMetadataMappingEntityMgr exportFieldMetadataMappingEntityMgr;

    @Override
    public BaseDao<LookupIdMap> getDao() {
        return lookupIdMappingEntityMgrDao;
    }

    @Override
    public BaseJpaRepository<LookupIdMap, Long> getRepository() {
        return lookupIdMappingRepository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<LookupIdMap> getLookupIdMappings(CDLExternalSystemType externalSystemType, String sortby,
            boolean descending) {
        if (StringUtils.isNotEmpty(sortby)) {
            sortby = sortby.trim();
        } else {
            sortby = "updated";
        }
        Sort sort = new Sort(descending ? Direction.DESC : Direction.ASC, sortby);
        return lookupIdMappingRepository.findAll(sort);

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public LookupIdMap createExternalSystem(LookupIdMap lookupIdsMap) {
        checkValidOrgName(lookupIdsMap);

        Tenant tenant = MultiTenantContext.getTenant();
        lookupIdsMap.setTenant(tenant);
        lookupIdsMap.setId(UUID.randomUUID().toString());
        lookupIdsMap.setIsRegistered(true);
        getDao().create(lookupIdsMap);

        if (lookupIdsMap.getExternalAuthentication() != null) {
            lookupIdsMap.getExternalAuthentication().setLookupIdMap(lookupIdsMap);
            extSysAuthenticationDao.create(lookupIdsMap.getExternalAuthentication());
        }

        if (lookupIdsMap.getExportFieldMetadataMappings() != null) {
            List<ExportFieldMetadataMapping> exportFieldMetadataMappings = lookupIdsMap
                    .getExportFieldMetadataMappings();
            exportFieldMetadataMappings.forEach(mapping -> {
                mapping.setTenant(tenant);
                mapping.setLookupIdMap(lookupIdsMap);
            });

            exportFieldMetadataMappingDao.create(exportFieldMetadataMappings, true);
        }
        return lookupIdsMap;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public LookupIdMap getLookupIdMap(String id) {
        return lookupIdMappingRepository.findById(id);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public LookupIdMap getLookupIdMap(String orgId, CDLExternalSystemType externalSystemType) {
        return lookupIdMappingRepository.findByOrgIdAndExternalSystemType(orgId, externalSystemType);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public LookupIdMap updateLookupIdMap(LookupIdMap lookupIdMap) {
        Tenant tenant = MultiTenantContext.getTenant();

        if (lookupIdMap.getExternalAuthentication() != null) {
            if (StringUtils.isBlank(lookupIdMap.getExternalAuthentication().getId())) {
                throw new LedpException(LedpCode.LEDP_40071);
            }
            ExternalSystemAuthentication updatedAuth = //
                    extSysAuthenticationDao.updateAuthentication(lookupIdMap.getExternalAuthentication());
            lookupIdMap.setExternalAuthentication(updatedAuth);
        }

        checkValidOrgName(lookupIdMap);

        if (lookupIdMap.getExportFieldMetadataMappings() != null) {
            List<ExportFieldMetadataMapping> updatedFieldMetadataMapping = exportFieldMetadataMappingEntityMgr
                    .update(lookupIdMap);

            lookupIdMap.setExportFieldMappings(updatedFieldMetadataMapping);
        }

        lookupIdMap.setUpdated(new Date(System.currentTimeMillis()));
        lookupIdMap.setTenant(tenant);
        getDao().update(lookupIdMap);

        return lookupIdMap;
    }

    private void checkValidOrgName(LookupIdMap lookupIdMap) {
        if (StringUtils.isBlank(lookupIdMap.getOrgName())) {
            throw new LedpException(LedpCode.LEDP_40080);
        }

        List<LookupIdMap> lookupIdMapWithSameOrgNameList = lookupIdMappingRepository.findAllByOrgName(lookupIdMap.getOrgName());

        for (LookupIdMap lookupIdMapWithSameOrgName : lookupIdMapWithSameOrgNameList) {
            if (!lookupIdMapWithSameOrgName.getPid().equals(lookupIdMap.getPid())) {
                throw new LedpException(LedpCode.LEDP_40081);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteLookupIdMap(String id) {
        LookupIdMap existingLookupIdMap = lookupIdMappingRepository.findById(id);
        getDao().delete(existingLookupIdMap);
    }
}
