package com.latticeengines.proxy.exposed.propdata;

import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchStatusResponse;
import com.latticeengines.network.exposed.propdata.MatchCommandInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class MatchCommandProxy extends BaseRestApiProxy implements MatchCommandInterface {
    public MatchCommandProxy() {
        super("propdata/matchcommands");
    }

    @Override
    public MatchStatusResponse getMatchStatus(Long commandID, String clientName) {
        String url = constructUrl("/{commandID}?client={clientName}", commandID, clientName);
        RetryTemplate retry = getRetryTemplate(1000, 10);
        return get("getMatchStatus", url, MatchStatusResponse.class, retry);
    }

    @Override
    public Commands createMatchCommand(CreateCommandRequest request, String clientName) {
        String url = constructUrl("?client={clientName}", clientName);
        RetryTemplate retry = getRetryTemplate(1000, 10);
        return post("createMatchCommand", url, request, Commands.class, retry);
    }

    @Override
    public MatchClientDocument getBestMatchClient(int numRows) {
        String url = constructUrl("bestclient?rows={numRows}", numRows);
        RetryTemplate retry = getRetryTemplate(1000, 10);
        return get("getBestMatchClient", url, MatchClientDocument.class, retry);
    }
}
