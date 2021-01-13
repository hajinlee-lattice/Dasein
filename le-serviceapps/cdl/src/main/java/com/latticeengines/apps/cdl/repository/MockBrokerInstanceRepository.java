package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.MockBrokerInstance;
import com.latticeengines.domain.exposed.security.Tenant;

public interface MockBrokerInstanceRepository extends BaseJpaRepository<MockBrokerInstance, Long> {

    @Query(value = "select * from MOCK_BROKER_INSTANCE order by PID desc limit :maxRow", nativeQuery = true)
    List<MockBrokerInstance> findAllWithLimit(@Param("maxRow") int maxRow);

    MockBrokerInstance findByTenantAndSourceId(@Param("tenant") Tenant tenant, @Param("sourceId") String sourceId);

}
