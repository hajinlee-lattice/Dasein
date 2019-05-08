package com.latticeengines.apps.cdl.repository;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.AtlasExport;

public interface AtlasExportRepository extends BaseJpaRepository<AtlasExport, Long> {

    AtlasExport findByUuid(String uuid);
}
