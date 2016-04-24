package com.latticeengines.proxy.exposed.propdata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
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
    public MatchOutput matchRealTime(MatchInput input) {
        String url = constructUrl("/realtime");
        return post("realtime_match", url, input, MatchOutput.class);
    }

    @Override
    public MatchCommand matchBulk(MatchInput matchInput, String hdfsPod) {
        String url = constructUrl("/bulk?podid={pod}", hdfsPod);
        return post("bulk_match", url, matchInput, MatchCommand.class);
    }

    @Override
    public MatchCommand bulkMatchStatus(String rootuid) {
        String url = constructUrl("/bulk/{rootuid}", rootuid);
        return get("bulk_status", url, MatchCommand.class);
    }

}
