package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.CreateCommandRequest;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchStatusResponse;

public interface MatchCommandInterface {
    MatchStatusResponse getMatchStatus(Long commandID, String clientName);

    Commands createMatchCommand(CreateCommandRequest request, String clientName);

    MatchClientDocument getBestMatchClient(int numRows);
}
