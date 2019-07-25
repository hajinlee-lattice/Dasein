package com.latticeengines.metadata.entitymgr.impl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.dao.MigrationTrackDao;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.metadata.repository.db.MigrationTrackRepository;

@Component("migrationTrackEntityMgr")
public class MigrationTrackEntityMgrImpl extends BaseEntityMgrRepositoryImpl<MigrationTrack, Long>
        implements MigrationTrackEntityMgr {

    @Autowired
    private MigrationTrackDao migrationTrackDao;

    @Autowired
    private MigrationTrackRepository migrationTrackRepository;

    @Override
    public BaseDao<MigrationTrack> getDao() {
        return migrationTrackDao;
    }

    @Override
    public BaseJpaRepository<MigrationTrack, Long> getRepository() {
        return migrationTrackRepository;
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    @Override
    public MigrationTrack findByTenant(Tenant tenant) {
        return migrationTrackRepository.findByTenant(tenant);
    }

    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public boolean tenantInMigration(Tenant tenant) {
        MigrationTrack track = findByTenant(tenant);
        return track != null && track.getStatus() == MigrationTrack.Status.STARTED;
    }

    @Override
    public boolean canDeleteOrRenameTable(Tenant tenant, String tableName) {
        MigrationTrack track = findByTenant(tenant);
        if (track != null && track.getStatus() == MigrationTrack.Status.STARTED) {
            List<String> tableNames = track.getCurActiveTable().values().stream().flatMap(Arrays::stream).collect(Collectors.toList());
            return !tableNames.contains(tableName);
        }
        return true;
    }
}
