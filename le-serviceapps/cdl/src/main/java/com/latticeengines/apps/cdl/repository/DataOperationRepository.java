package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataOperation;

public interface DataOperationRepository extends BaseJpaRepository<DataOperation, Long> {

    DataOperation findByDropPath(String dropPath);
}
