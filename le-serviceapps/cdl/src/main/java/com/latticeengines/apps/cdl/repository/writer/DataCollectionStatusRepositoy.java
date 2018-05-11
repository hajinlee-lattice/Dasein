package com.latticeengines.apps.cdl.repository.writer;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionStatusRepositoy extends BaseJpaRepository<DataCollectionStatus, Long> {

    DataCollectionStatus findByTenant(Tenant tenant);
}
