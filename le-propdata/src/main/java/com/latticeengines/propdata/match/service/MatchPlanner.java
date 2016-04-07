package com.latticeengines.propdata.match.service;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchPlanner {

    MatchContext plan(MatchInput input);

    void generateInputMetric(MatchInput input);

}
