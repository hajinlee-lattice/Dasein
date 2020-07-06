package com.latticeengines.apps.lp.repository.reader;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;

public interface ScoringRequestConfigReaderRepository extends BaseJpaRepository<ScoringRequestConfig, Long> {

    @Query(name = ScoringRequestConfig.NQ_SCORING_REQUEST_CONTEXT_BY_CONFIG_ID)
    ScoringRequestConfigContext retrieveScoringRequestConfigContext(@Param("configId") String configUuid);

}
