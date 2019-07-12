package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    @Override
    public MigrationTrack findByTenant(Tenant tenant) {
        return migrationTrackRepository.findByTenant(tenant);
    }
}
