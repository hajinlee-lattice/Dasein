package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;

public interface ExternalSystemAuthenticationRepository
        extends BaseJpaRepository<ExternalSystemAuthentication, Long> {

    @Query(name = ExternalSystemAuthentication.NQ_FIND_AUTHS_BY_LOOKUPMAP_IDS)
    List<ExternalSystemAuthentication> findByLookupMapIds(@Param("lookupMapIds")List<String> lookupMapIds);

    @Query(name = ExternalSystemAuthentication.NQ_FIND_AUTHS_BY_AUTH_ID)
    ExternalSystemAuthentication findByAuthId(@Param("authId")String authId);

    @Query(name = ExternalSystemAuthentication.NQ_FIND_ALL_AUTHS)
    List<ExternalSystemAuthentication> findAllAuths();

    @Query(name = ExternalSystemAuthentication.NQ_FIND_AUTHS_BY_TRAY_AUTH_ID)
    List<ExternalSystemAuthentication> retrieveAllByTrayAuthId(@Param("trayAuthId")String trayAuthId);
}
