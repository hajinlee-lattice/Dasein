package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.CreateCommandRequest;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchStatusResponse;
import com.latticeengines.network.exposed.propdata.MatchCommandInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class MatchCommandProxy extends BaseRestApiProxy implements MatchCommandInterface {
    public MatchCommandProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/matchcommands");
    }

    @Override
    public MatchStatusResponse getMatchStatus(Long commandID, String clientName) {
        String url = constructUrl("/{commandID}?client={clientName}", commandID, clientName);
        return get("getMatchStatus", url, MatchStatusResponse.class);
    }

    @Override
    public Commands createMatchCommand(CreateCommandRequest request, String clientName) {
        String url = constructUrl("?client={clientName}", clientName);
        return post("createMatchCommand", url, request, Commands.class);
    }

    @Override
    public MatchClientDocument getBestMatchClient(int numRows) {
        String url = constructUrl("bestclient?rows={numRows}", numRows);
        return get("getBestMatchClient", url, MatchClientDocument.class);
    }
}
