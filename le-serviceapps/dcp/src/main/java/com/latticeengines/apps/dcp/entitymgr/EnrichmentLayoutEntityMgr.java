package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;

public interface EnrichmentLayoutEntityMgr extends BaseEntityMgrRepository<EnrichmentLayout, Long> {

    List<EnrichmentLayout> findAll(Pageable pageable);

    EnrichmentLayout findByLayoutId(String layoutId);

    EnrichmentLayout findBySourceId(String sourceId);


}
