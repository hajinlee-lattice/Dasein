package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.metadata.dao.ArtifactDao;

@Component("artifactDao")
public class ArtifactDaoImpl extends BaseDaoImpl<Artifact> implements ArtifactDao {

    @Override
    protected Class<Artifact> getEntityClass() {
        return Artifact.class;
    }

}
