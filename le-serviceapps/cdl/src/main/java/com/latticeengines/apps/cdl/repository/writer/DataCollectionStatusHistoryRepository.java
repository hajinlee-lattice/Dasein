package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DataCollectionStatusHistoryRepository extends BaseJpaRepository<DataCollectionStatusHistory, Long> {

    List<DataCollectionStatusHistory> findByTenantOrderByCreationTimeDesc(Tenant tenant);

}
