package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Artifact;

public interface ArtifactEntityMgr extends BaseEntityMgr<Artifact> {

    public Artifact findByPath(String path);
}
