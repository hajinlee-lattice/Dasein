package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

public interface BulkMatchService {
    boolean accept(String version);

    MatchCommand match(MatchInput input, String hdfsPodId);

    MatchCommand status(String rootOperationUid);

}
