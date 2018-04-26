package com.latticeengines.apps.cdl.repository.writer;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface LookupIdMappingRepository extends BaseJpaRepository<LookupIdMap, Long> {

    public LookupIdMap findById(String id);

}
