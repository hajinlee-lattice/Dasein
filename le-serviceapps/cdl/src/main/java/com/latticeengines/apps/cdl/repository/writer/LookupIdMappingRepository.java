package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface LookupIdMappingRepository
        extends BaseJpaRepository<LookupIdMap, Long>, JpaSpecificationExecutor<LookupIdMap> {

    LookupIdMap findById(String id);

    LookupIdMap findByOrgIdAndExternalSystemType(String orgId, CDLExternalSystemType externalSystemType);

    List<LookupIdMap> findAllByOrgName(String orgName);

    List<LookupIdMap> findAllByExternalSystemName(CDLExternalSystemName externalSystemName);

    @Query("SELECT l FROM LookupIdMap l WHERE l.externalAuthentication is not null "
            + "and l.externalAuthentication.id = :extSystemAuthId")
    LookupIdMap retrieveByExternalSystemAuthentication(@Param("extSystemAuthId") String externalSystemAuthId);
}
