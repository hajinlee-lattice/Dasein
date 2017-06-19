package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.network.exposed.propdata.MatchInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("matchProxy")
public class MatchProxy extends BaseRestApiProxy implements MatchInterface {

    public MatchProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/matches");
    }

    public MatchOutput matchRealTime(MatchInput input) {
        String url = constructUrl("/realtime");
        return post("realtime_match", url, input, MatchOutput.class);
    }

    public MatchCommand matchBulk(MatchInput matchInput, String hdfsPod) {
        String url = constructUrl("/bulk?podid={pod}", hdfsPod);
        return post("bulk_match", url, matchInput, MatchCommand.class);
    }

    public BulkMatchWorkflowConfiguration getBulkConfig(MatchInput matchInput, String hdfsPod) {
        String url = constructUrl("/bulkconf?podid={pod}", hdfsPod);
        return post("bulk_match_conf", url, matchInput, BulkMatchWorkflowConfiguration.class);
    }

    public MatchCommand bulkMatchStatus(String rootuid) {
        String url = constructUrl("/bulk/{rootuid}", rootuid);
        return get("bulk_status", url, MatchCommand.class);
    }

    public BulkMatchOutput matchRealTime(BulkMatchInput input) {
        String url = constructUrl("/bulkrealtime");
        return post("bulkrealtime_match", url, input, BulkMatchOutput.class);
    }

}
