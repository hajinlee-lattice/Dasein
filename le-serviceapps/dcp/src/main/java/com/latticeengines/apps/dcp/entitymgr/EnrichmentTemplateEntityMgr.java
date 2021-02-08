package com.latticeengines.apps.dcp.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

public interface EnrichmentTemplateEntityMgr extends BaseEntityMgrRepository<EnrichmentTemplate, Long> {
    List<EnrichmentTemplate> findAll(Pageable pageable);

    EnrichmentTemplate find(String templateId);
}
