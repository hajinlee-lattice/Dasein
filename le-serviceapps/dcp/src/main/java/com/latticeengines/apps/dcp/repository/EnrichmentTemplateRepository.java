package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

public interface EnrichmentTemplateRepository  extends BaseJpaRepository<EnrichmentTemplate, Long> {

    @Query("SELECT et FROM EnrichmentTemplate AS et")
    List<EnrichmentTemplate> findAllEnrichmentTemplates(Pageable pageable);
}
