package com.latticeengines.apps.dcp.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;

public interface EnrichmentLayoutRepository extends BaseJpaRepository<EnrichmentLayout, Long> {

    @Query("SELECT el FROM EnrichmentLayout AS el WHERE el.deleted = false")
    List<EnrichmentLayout> findAllEnrichmentLayouts(Pageable pageable);

    @Query("SELECT el FROM EnrichmentLayout AS el")
    List<EnrichmentLayout> findAllEnrichmentLayoutsIncludeArchived(Pageable pageable);
}
