package com.latticeengines.pls.repository.reader;

import java.util.List;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.EntityGraph.EntityGraphType;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;

public interface ScoringRequestConfigReaderRepository extends BaseJpaRepository<ScoringRequestConfig, Long> {

    @EntityGraph(value="ScoringRequestConfig.details", type=EntityGraphType.LOAD)
    ScoringRequestConfig findByMarketoCredentialPidAndConfigId(Long marketoCredentialPid, String configId);

    @EntityGraph(value="ScoringRequestConfig.details", type=EntityGraphType.LOAD)
    ScoringRequestConfig findByMarketoCredentialPidAndModelUuid(Long marketoCredentialPid, String modelUuid);

    @Query(name = ScoringRequestConfig.NQ_FIND_CONFIGS_BY_CREDENTIAL_ID)
    List<ScoringRequestConfigSummary> findByMarketoCredentialPid(@Param("credentialPid")Long credentialPid);

    @Query(name = ScoringRequestConfig.NQ_SCORING_REQUEST_CONTEXT_BY_CONFIG_ID)
    ScoringRequestConfigContext retrieveScoringRequestConfigContext(@Param("configId")String configUuid);

}
