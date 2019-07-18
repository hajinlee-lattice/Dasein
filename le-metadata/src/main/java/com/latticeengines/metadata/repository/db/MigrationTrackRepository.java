package com.latticeengines.metadata.repository.db;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.security.Tenant;

@Repository
@Component("migrationTrackRepository")
public interface MigrationTrackRepository extends BaseJpaRepository<MigrationTrack, Long> {
    @Query("SELECT m FROM MigrationTrack m WhERE m.tenant = :tenant")
    MigrationTrack findByTenant(@Param("tenant") Tenant tenant);
}
