package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface LookupIdMappingEntityMgr {

    List<LookupIdMap> getLookupIdMappings(CDLExternalSystemType externalSystemType, String sortby, boolean descending);

    LookupIdMap createExternalSystem(LookupIdMap lookupIdsMap);

    LookupIdMap getLookupIdMap(String id);

    LookupIdMap updateLookupIdMap(LookupIdMap lookupIdMap);

    void deleteLookupIdMap(String id);

    LookupIdMap getLookupIdMap(String orgId, CDLExternalSystemType externalSystemType);

    LookupIdMap retrieveLookupIdMapByExtSysAuth(String externalSystemAuthId);

    List<LookupIdMap> getLookupIdMapsByExtSysName(CDLExternalSystemName externalSystemName);

}
