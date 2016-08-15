package com.latticeengines.proxy.exposed.propdata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchStatusResponse;
import com.latticeengines.network.exposed.propdata.MatchCommandInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class MatchCommandProxy extends MicroserviceRestApiProxy implements MatchCommandInterface {
    public MatchCommandProxy() {
        super("propdata/matchcommands");
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
