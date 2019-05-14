package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface LookupIdMappingEntityMgr {

    List<LookupIdMap> getLookupIdsMapping(CDLExternalSystemType externalSystemType, String sortby, boolean descending);

    LookupIdMap createExternalSystem(LookupIdMap lookupIdsMap);

    LookupIdMap getLookupIdMap(String id);

    LookupIdMap updateLookupIdMap(String id, LookupIdMap lookupIdMap);

    void deleteLookupIdMap(String id);

    LookupIdMap getLookupIdMap(String orgId, CDLExternalSystemType externalSystemType);

}
