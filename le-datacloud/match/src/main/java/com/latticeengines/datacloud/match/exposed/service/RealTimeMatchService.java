package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;

public interface RealTimeMatchService {

    MatchOutput match(MatchInput input);

    BulkMatchOutput matchBulk(BulkMatchInput input);

}
