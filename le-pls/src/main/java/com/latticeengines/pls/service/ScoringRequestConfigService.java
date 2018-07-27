package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;

public interface ScoringRequestConfigService {

    void createScoringRequestConfig(ScoringRequestConfig scoringRequestConfig);
    
    List<ScoringRequestConfigSummary> findAllByMarketoCredential(Long credentialPid);
    
    ScoringRequestConfig findByModelUuid(Long credentialPid, String modelUuid);
    
    ScoringRequestConfig findByConfigId(Long credentialPid, String configId);
    
    void updateScoringRequestConfig(ScoringRequestConfig scoringRequestConfig);
}
