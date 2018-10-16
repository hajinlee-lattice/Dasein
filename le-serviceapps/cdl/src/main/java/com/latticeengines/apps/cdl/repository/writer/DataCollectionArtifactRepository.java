package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionArtifactRepository extends BaseJpaRepository<DataCollectionArtifact, Long> {
    List<DataCollectionArtifact> findByTenantAndVersion(Tenant tenant, DataCollection.Version version);

    DataCollectionArtifact findByTenantAndNameAndVersion(Tenant tenant, String name, DataCollection.Version version);
}
