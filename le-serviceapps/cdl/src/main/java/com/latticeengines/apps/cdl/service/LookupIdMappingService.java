package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface LookupIdMappingService {

    Map<String, List<LookupIdMap>> getLookupIdsMapping(CDLExternalSystemType externalSystemType, String sortby,
            boolean descending);

    LookupIdMap registerExternalSystem(LookupIdMap lookupIdsMap);

    void deregisterExternalSystem(LookupIdMap lookupIdMap);

    LookupIdMap getLookupIdMap(String id);

    LookupIdMap updateLookupIdMap(String id, LookupIdMap lookupIdMap);

    void deleteLookupIdMap(String id);

    Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(CDLExternalSystemType externalSystemType);

    List<CDLExternalSystemType> getAllCDLExternalSystemType();

    LookupIdMap getLookupIdMapByOrgId(String orgId, CDLExternalSystemType externalSystemType);
}
