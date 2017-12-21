package com.latticeengines.auth.exposed.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserConfigSummary;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantConfig;

public interface GlobalAuthUserTenantConfigRepository extends BaseJpaRepository<GlobalAuthUserTenantConfig, Long> {

    List<GlobalAuthUserTenantConfig> findByGlobalAuthUserPidAndGlobalAuthTenantPid(Long userId, Long tenantId);

    List<GlobalAuthUserTenantConfig> findByGlobalAuthUserPid(Long userId);

    @Query(name = GlobalAuthUserTenantConfig.NQ_FIND_CONFIG_BY_USER_ID)
    List<GlobalAuthUserConfigSummary> findGlobalAuthUserConfigSummaryByUserId(@Param("userId") Long userId);

    @Query(name = GlobalAuthUserTenantConfig.NQ_FIND_CONFIG_BY_USER_ID_TENANT_ID)
    List<GlobalAuthUserConfigSummary> findGlobalAuthUserConfigSummaryByUserIdTenantId(@Param("userId") Long userId, @Param("tenantId") Long tenantId);
}
