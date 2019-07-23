package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.metadata.dao.MigrationTrackDao;

@Component("migrationTrackDao")
public class MigrationTrackDaoImpl extends BaseDaoImpl<MigrationTrack> implements MigrationTrackDao {
    @Override
    protected Class<MigrationTrack> getEntityClass() {
        return MigrationTrack.class;
    }
}
