package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.LookupIdMappingDao;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.LookupIdMappingRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("lookupIdMappingEntityMgr")
public class LookupIdMappingEntityMgrImpl extends BaseEntityMgrRepositoryImpl<LookupIdMap, Long>
        implements LookupIdMappingEntityMgr {

    @Inject
    private LookupIdMappingDao lookupIdMappingEntityMgrDao;

    @Inject
    private LookupIdMappingRepository lookupIdMappingRepository;

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
    public Map<String, List<LookupIdMap>> getLookupIdsMapping(CDLExternalSystemType externalSystemType, String sortby,
            boolean descending) {
        if (StringUtils.isNotEmpty(sortby)) {
            sortby = sortby.trim();
        } else {
            sortby = "updated";
        }
        Sort sort = new Sort(descending ? Direction.DESC : Direction.ASC, sortby);
        List<LookupIdMap> configs = lookupIdMappingRepository.findAll(sort);
        Map<String, List<LookupIdMap>> result = new HashMap<>();
        if (CollectionUtils.isNotEmpty(configs)) {
            configs.stream() //
                    .filter(c -> externalSystemType == null || c.getExternalSystemType() == externalSystemType)
                    .forEach(c -> {
                        CDLExternalSystemType type = c.getExternalSystemType();
                        List<LookupIdMap> listForType = result.get(type.name());
                        if (listForType == null) {
                            listForType = new ArrayList<>();
                            result.put(type.name(), listForType);
                        }
                        listForType.add(c);
                    });
        }
        return result;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public LookupIdMap createExternalSystem(LookupIdMap lookupIdsMap) {
        Tenant tenant = MultiTenantContext.getTenant();
        lookupIdsMap.setTenant(tenant);
        lookupIdsMap.setId(UUID.randomUUID().toString());
        Date time = new Date(System.currentTimeMillis());
        lookupIdsMap.setCreated(time);
        lookupIdsMap.setUpdated(time);
        lookupIdsMap.setIsRegistered(true);
        getDao().create(lookupIdsMap);
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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public LookupIdMap updateLookupIdMap(String id, LookupIdMap lookupIdMap) {
        Tenant tenant = MultiTenantContext.getTenant();
        lookupIdMap.setUpdated(new Date(System.currentTimeMillis()));
        lookupIdMap.setTenant(tenant);
        getDao().update(lookupIdMap);
        return lookupIdMap;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public void deleteLookupIdMap(String id) {
        LookupIdMap existingLookupIdMap = lookupIdMappingRepository.findById(id);
        getDao().delete(existingLookupIdMap);
    }

}
