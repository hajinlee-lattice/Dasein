package com.latticeengines.propdata.match.service;

import com.latticeengines.domain.exposed.propdata.match.BulkMatchInput;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;

public interface RealTimeMatchService {

    MatchOutput match(MatchInput input);

    BulkMatchOutput match(BulkMatchInput input);

}
