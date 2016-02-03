package com.latticeengines.proxy.exposed.propdata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.network.exposed.propdata.MatchInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class MatchProxy extends BaseRestApiProxy implements MatchInterface {

    public MatchProxy() {
        super("propdata/matches");
    }

    @Override
    public MatchOutput match(MatchInput input, Boolean returnUnmatched) {
        String url = constructUrl("?unmatched={unmatched}", String.valueOf(returnUnmatched));
        return post("match", url, input, MatchOutput.class);
    }

}
