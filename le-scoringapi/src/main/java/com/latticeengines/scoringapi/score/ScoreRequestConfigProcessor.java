package com.latticeengines.scoringapi.score;

import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;

public interface ScoreRequestConfigProcessor {

    ScoringRequestConfigContext getScoreRequestConfigContext(String configId, String secretKey);
    
    ScoreRequest preProcessScoreRequestConfig(ScoreRequest scoreRequest, ScoringRequestConfigContext srcContext);

}
