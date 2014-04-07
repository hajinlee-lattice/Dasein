package com.latticeengines.scoring.exposed.service;

import java.util.List;

import org.dmg.pmml.PMML;

import com.latticeengines.scoring.exposed.domain.ScoringRequest;
import com.latticeengines.scoring.exposed.domain.ScoringResponse;

public interface ScoringService {

    List<ScoringResponse> scoreBatch(List<ScoringRequest> scoringRequests, PMML pmml);
    
    ScoringResponse score(ScoringRequest scoringRequest, PMML pmml);
}
