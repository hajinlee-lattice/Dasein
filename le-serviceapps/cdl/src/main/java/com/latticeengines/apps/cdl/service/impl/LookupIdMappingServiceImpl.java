package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("lookupIdMappingService")
public class LookupIdMappingServiceImpl implements LookupIdMappingService {
    private static final Logger log = LoggerFactory.getLogger(LookupIdMappingServiceImpl.class);

    @Inject
    private CDLExternalSystemService externalSystemService;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Override
    public Map<String, List<LookupIdMap>> getLookupIdsMapping(CDLExternalSystemType externalSystemType, String sortby,
            boolean descending) {
        return lookupIdMappingEntityMgr.getLookupIdsMapping(externalSystemType, sortby, descending);
    }

    @Override
    public LookupIdMap registerExternalSystem(LookupIdMap lookupIdsMap) {
        LookupIdMap existingLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(lookupIdsMap.getOrgId(),
                lookupIdsMap.getExternalSystemType());
        if (existingLookupIdMap == null) {
            return lookupIdMappingEntityMgr.createExternalSystem(lookupIdsMap);
        } else {
            existingLookupIdMap.setIsRegistered(true);
            return lookupIdMappingEntityMgr.updateLookupIdMap(existingLookupIdMap.getId(), existingLookupIdMap);
        }
    }

    @Override
    public void deregisterExternalSystem(LookupIdMap lookupIdsMap) {
        if (lookupIdsMap == null) {
            return;
        }

        LookupIdMap existingLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(lookupIdsMap.getOrgId(),
                lookupIdsMap.getExternalSystemType());
        if (existingLookupIdMap != null && existingLookupIdMap.getIsRegistered()) {
            existingLookupIdMap.setIsRegistered(false);
            lookupIdMappingEntityMgr.updateLookupIdMap(existingLookupIdMap.getId(), existingLookupIdMap);
        }
    }

    @Override
    public LookupIdMap getLookupIdMap(String id) {
        return lookupIdMappingEntityMgr.getLookupIdMap(id);
    }

    @Override
    public LookupIdMap updateLookupIdMap(String id, LookupIdMap lookupIdMap) {
        LookupIdMap existingLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(id);
        if (existingLookupIdMap != null) {
            if (lookupIdMap != null) {
                existingLookupIdMap.setAccountId(lookupIdMap.getAccountId());
                existingLookupIdMap.setDescription(lookupIdMap.getDescription());
                existingLookupIdMap.setExternalAuthentication(lookupIdMap.getExternalAuthentication());
            } else {
                throw new RuntimeException(
                        "Incorrect input payload. Will replace this exception with proper LEDP exception.");
            }
        } else {
            throw new RuntimeException(String.format("No registration exists for id %s yet, update not allowed. "
                    + "Will replace this exception with proper LEDP exception.", id));
        }

        return lookupIdMappingEntityMgr.updateLookupIdMap(id, existingLookupIdMap);
    }

    @Override
    public void deleteLookupIdMap(String id) {
        lookupIdMappingEntityMgr.deleteLookupIdMap(id);
    }

    @Override
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(CDLExternalSystemType externalSystemType) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        Map<String, List<CDLExternalSystemMapping>> result;
        try {
            if (externalSystemType == null) {
                result = externalSystemService.getExternalSystemMap(space.toString(), BusinessEntity.Account);
            } else {
                result = new HashMap<>();
                result.put(externalSystemType.name(), externalSystemService.getExternalSystemByType( //
                        space.toString(), externalSystemType, BusinessEntity.Account));
            }
        } catch (Exception ex) {
            result = new HashMap<>();
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.CRM) {
                result.put(CDLExternalSystemType.CRM.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.MAP) {
                result.put(CDLExternalSystemType.MAP.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.ERP) {
                result.put(CDLExternalSystemType.ERP.name(), new ArrayList<>());
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.OTHER) {
                result.put(CDLExternalSystemType.OTHER.name(), new ArrayList<>());
            }
            log.error("Ignoring this error for now and returning default map of empty lists", ex);
        }

        return result;
    }

    @Override
    public List<CDLExternalSystemType> getAllCDLExternalSystemType() {
        return Arrays.asList(CDLExternalSystemType.values());
    }
}
