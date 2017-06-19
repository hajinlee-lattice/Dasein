package com.latticeengines.matchapi.service;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;

public interface BulkMatchService {
    boolean accept(String version);

    MatchCommand match(MatchInput input, String hdfsPodId);

    MatchCommand status(String rootOperationUid);

    BulkMatchWorkflowConfiguration getWorkflowConf(MatchInput input, String hdfsPodId);

}
