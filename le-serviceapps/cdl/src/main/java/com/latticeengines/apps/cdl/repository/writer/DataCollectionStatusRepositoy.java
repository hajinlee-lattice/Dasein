package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionStatusRepositoy extends BaseJpaRepository<DataCollectionStatus, Long> {

    List<DataCollectionStatus> findByTenantAndVersion(Tenant tenant, DataCollection.Version version);
}
