package com.latticeengines.scoringapi.score;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreType;

public interface ScoreRequestProcessor {

    ScoreResponse process(CustomerSpace space, ScoreRequest request, ScoreType scoreType);
}
