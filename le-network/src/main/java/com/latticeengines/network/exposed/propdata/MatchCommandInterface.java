package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchStatusResponse;

public interface MatchCommandInterface {
    MatchStatusResponse getMatchStatus(Long commandID, String clientName);

    Commands createMatchCommand(CreateCommandRequest request, String clientName);

    MatchClientDocument getBestMatchClient();
}
