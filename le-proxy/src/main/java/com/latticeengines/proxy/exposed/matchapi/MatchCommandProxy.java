package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.CreateCommandRequest;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchStatusResponse;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

// SQL Server based match: not supported anymore
@Deprecated
@Component
public class MatchCommandProxy extends BaseRestApiProxy {
    public MatchCommandProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/matchcommands");
    }

    public MatchStatusResponse getMatchStatus(Long commandID, String clientName) {
        String url = constructUrl("/{commandID}?client={clientName}", commandID, clientName);
        return get("getMatchStatus", url, MatchStatusResponse.class);
    }

    public Commands createMatchCommand(CreateCommandRequest request, String clientName) {
        String url = constructUrl("?client={clientName}", clientName);
        return post("createMatchCommand", url, request, Commands.class);
    }

    public MatchClientDocument getBestMatchClient(int numRows) {
        String url = constructUrl("bestclient?rows={numRows}", numRows);
        return get("getBestMatchClient", url, MatchClientDocument.class);
    }
}
