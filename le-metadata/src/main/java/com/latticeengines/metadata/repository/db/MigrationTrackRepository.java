package com.latticeengines.metadata.repository.db;

import java.util.List;

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

    @Query("SELECT m FROM MigrationTrack m WHERE m.tenant = :tenant")
    MigrationTrack findByTenant(@Param("tenant") Tenant tenant);

    @Query("SELECT m.tenant.pid FROM MigrationTrack m WHERE m.status = :status")
    List<Long> findByStatus(@Param("status") MigrationTrack.Status status);
}
