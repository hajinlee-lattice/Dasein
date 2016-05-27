package com.latticeengines.scoring.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;

public interface ScoringService {

    ApplicationId submitScoreWorkflow(RTSBulkScoringConfiguration rtsBulkScoringConfig);
}
