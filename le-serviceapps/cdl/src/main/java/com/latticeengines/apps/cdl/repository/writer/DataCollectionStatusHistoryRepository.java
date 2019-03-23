package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;

public interface DataCollectionStatusHistoryRepository extends BaseJpaRepository<DataCollectionStatusHistory, Long> {

    List<DataCollectionStatusHistory> findByTenantNameOrderByCreationTimeDesc(String tenantName);

}
