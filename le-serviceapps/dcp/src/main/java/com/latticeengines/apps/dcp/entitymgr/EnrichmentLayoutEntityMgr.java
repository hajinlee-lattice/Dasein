package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;

public interface EnrichmentLayoutEntityMgr extends BaseEntityMgrRepository<EnrichmentLayout, Long> {

    List<EnrichmentLayoutDetail> findAllEnrichmentLayoutDetail(Pageable pageable);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailByLayoutId(String layoutId);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailBySourceId(String sourceId);

}
