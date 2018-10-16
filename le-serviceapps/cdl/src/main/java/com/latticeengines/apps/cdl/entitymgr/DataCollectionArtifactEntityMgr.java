package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionArtifactEntityMgr extends BaseEntityMgrRepository<DataCollectionArtifact, Long> {
    List<DataCollectionArtifact> findByTenantAndVersion(Tenant tenant, DataCollection.Version version);

    DataCollectionArtifact findByTenantAndNameAndVersion(Tenant tenant, String name, DataCollection.Version version);


}
