package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;

public interface ConvertBatchStoreInfoRepository extends BaseJpaRepository<ConvertBatchStoreInfo, Long> {

    ConvertBatchStoreInfo findByPid(Long pid);
}
