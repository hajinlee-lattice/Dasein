package com.latticeengines.datafabric.entitymanager;

import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricRecord;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricStatus;

public interface GenericFabricEntityManager extends BaseFabricEntityMgr<GenericFabricRecord> {

    GenericFabricStatus getBatchStatus(String batchId);

    String createUniqueBatchId(Long totalCount);

    String createOrGetNamedBatchId(String batchId, Long totalCount, boolean createNew);

    void updateBatchCount(String batchId, long delta);

    boolean cleanup(String batchId);

}
