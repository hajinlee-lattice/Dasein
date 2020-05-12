package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface LookupIdMappingRepository
        extends BaseJpaRepository<LookupIdMap, Long>, JpaSpecificationExecutor<LookupIdMap> {

    LookupIdMap findById(String id);

    LookupIdMap findByOrgIdAndExternalSystemType(String orgId, CDLExternalSystemType externalSystemType);
    
    List<LookupIdMap> findAllByOrgName(String orgName);
}
