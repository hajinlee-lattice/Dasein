package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;

@Component("lookupIdMappingService")
public class LookupIdMappingServiceImpl implements LookupIdMappingService {
    private static final Logger log = LoggerFactory.getLogger(LookupIdMappingServiceImpl.class);

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

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
        if (existingLookupIdMap == null || !existingLookupIdMap.getIsRegistered()) {
            return;
        } else {
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
        Map<String, List<CDLExternalSystemMapping>> result = null;
        try {
            if (externalSystemType == null) {
                cdlExternalSystemProxy.getExternalSystemMap(space.toString());
            } else {
                result = new HashMap<>();
                result.put(externalSystemType.name(),
                        cdlExternalSystemProxy.getExternalSystemByType(space.toString(), externalSystemType));
            }
        } catch (Exception ex) {
            log.error("Ignoring this error for now", ex);
            result = new HashMap<>();
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.CRM) {
                CDLExternalSystemMapping c1 = new CDLExternalSystemMapping(InterfaceName.SalesforceAccountID.name(),
                        "String", InterfaceName.SalesforceAccountID.name());
                result.put(CDLExternalSystemType.CRM.name(), Arrays.asList(c1));
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.MAP) {
                CDLExternalSystemMapping m1 = new CDLExternalSystemMapping("MAP_Acc_Id_1", "String", "Id MAP_Acc_Id_1");
                CDLExternalSystemMapping m2 = new CDLExternalSystemMapping("MAP_Acc_Id_2", "String", "Id MAP_Acc_Id_2");
                result.put(CDLExternalSystemType.MAP.name(), Arrays.asList(m1, m2));
            }
            if (externalSystemType == null || externalSystemType == CDLExternalSystemType.OTHER) {
                CDLExternalSystemMapping o1 = new CDLExternalSystemMapping("OTHER_Acc_Id_1", "String",
                        "Id OTHER_Acc_Id_1");
                CDLExternalSystemMapping o2 = new CDLExternalSystemMapping("OTHER_Acc_Id_2", "String",
                        "Id OTHER_Acc_Id_2");
                result.put(CDLExternalSystemType.OTHER.name(), Arrays.asList(o1, o2));
            }
        }

        return result;
    }

    @Override
    public List<CDLExternalSystemType> getAllCDLExternalSystemType() {
        return Arrays.asList(CDLExternalSystemType.values());
    }
}
