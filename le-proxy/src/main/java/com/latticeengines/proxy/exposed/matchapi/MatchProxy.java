package com.latticeengines.proxy.exposed.matchapi;

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.network.exposed.propdata.MatchInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("matchProxy")
public class MatchProxy extends BaseRestApiProxy implements MatchInterface {

    private static final Set<String> MATCHAPI_RETRY_MESSAGES = ImmutableSet.of("Connection reset", "502 Bad Gateway");

    public MatchProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/matches");
        this.setRetryMessages(MATCHAPI_RETRY_MESSAGES);
    }

    @Override
    public MatchOutput matchRealTime(MatchInput input) {
        String url = constructUrl("/realtime");
        return postKryo("realtime_match", url, input, MatchOutput.class);
    }

    public BulkMatchOutput matchRealTime(BulkMatchInput input) {
        String url = constructUrl("/bulkrealtime");
        return postKryo("bulkrealtime_match", url, input, BulkMatchOutput.class);
    }

    @Override
    public MatchCommand matchBulk(MatchInput matchInput, String hdfsPod) {
        String url = constructUrl("/bulk?podid={pod}", hdfsPod);
        return post("bulk_match", url, matchInput, MatchCommand.class);
    }

    public BulkMatchWorkflowConfiguration getBulkConfig(MatchInput matchInput, String hdfsPod) {
        String url = constructUrl("/bulkconf?podid={pod}", hdfsPod);
        return post("bulk_match_conf", url, matchInput, BulkMatchWorkflowConfiguration.class);
    }

    @Override
    public MatchCommand bulkMatchStatus(String rootuid) {
        String url = constructUrl("/bulk/{rootuid}", rootuid);
        return get("bulk_status", url, MatchCommand.class);
    }

    @Override
    public EntityPublishStatistics publishEntity(EntityPublishRequest request) {
        String url = constructUrl("/entity/publish");
        return postKryo("publish_entity", url, request, EntityPublishStatistics.class);
    }

    @Override
    public List<EntityPublishStatistics> publishEntity(List<EntityPublishRequest> requests) {
        String url = constructUrl("/entity/publish/list");
        List<?> list = postKryo("publish_entity_list", url, requests, List.class);
        return JsonUtils.convertList(list, EntityPublishStatistics.class);
    }

    @Override
    public BumpVersionResponse bumpVersion(BumpVersionRequest request) {
        String url = constructUrl("/entity/versions");
        return postKryo("bump_version", url, request, BumpVersionResponse.class);
    }
}
