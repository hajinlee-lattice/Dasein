package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionArtifactRepository extends BaseJpaRepository<DataCollectionArtifact, Long> {
    List<DataCollectionArtifact> findByTenantAndVersionOrderByCreateTimeDesc(
            Tenant tenant, DataCollection.Version version);

    List<DataCollectionArtifact> findByTenantAndStatusAndVersionOrderByCreateTimeDesc(
            Tenant tenant, DataCollectionArtifact.Status status, DataCollection.Version version);

    List<DataCollectionArtifact> findByTenantAndNameAndVersionOrderByCreateTimeDesc(
            Tenant tenant, String name, DataCollection.Version version);
}
