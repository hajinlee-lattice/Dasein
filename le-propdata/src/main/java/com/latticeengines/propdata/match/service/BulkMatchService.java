package com.latticeengines.propdata.match.service;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;

public interface BulkMatchService {
    boolean accept(String version);

    MatchCommand match(MatchInput input, String hdfsPodId);

    MatchCommand status(String rootOperationUid);

}
