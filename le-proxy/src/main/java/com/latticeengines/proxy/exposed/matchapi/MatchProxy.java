package com.latticeengines.proxy.exposed.matchapi;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.InternalAccountIdLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.InternalAccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.InternalContactLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.ConfigurationRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchVersion;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("matchProxy")
public class MatchProxy extends BaseRestApiProxy {

    private static final Set<String> MATCHAPI_RETRY_MESSAGES = ImmutableSet.of("Connection reset", "502 Bad Gateway");

    public MatchProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/matches");
        this.setRetryMessages(MATCHAPI_RETRY_MESSAGES);
    }

    public MatchOutput matchRealTime(MatchInput input) {
        String url = constructUrl("/realtime");
        return postKryo("realtime_match", url, input, MatchOutput.class);
    }

    public BulkMatchOutput matchRealTime(BulkMatchInput input) {
        String url = constructUrl("/bulkrealtime");
        return postKryo("bulkrealtime_match", url, input, BulkMatchOutput.class);
    }

    public MatchCommand matchBulk(MatchInput matchInput, String hdfsPod) {
        String url = constructUrl("/bulk?podid={pod}", hdfsPod);
        return post("bulk_match", url, matchInput, MatchCommand.class);
    }

    public MatchCommand matchBulk(MatchInput matchInput, String hdfsPod, String rootOperationUid) {
        String url = constructUrl("/bulk?podid={pod}&rootuid={rootuid}", hdfsPod, rootOperationUid);
        return post("bulk_match", url, matchInput, MatchCommand.class);
    }

    public BulkMatchWorkflowConfiguration getBulkConfig(MatchInput matchInput, String hdfsPod) {
        String url = constructUrl("/bulkconf?podid={pod}", hdfsPod);
        return post("bulk_match_conf", url, matchInput, BulkMatchWorkflowConfiguration.class);
    }

    public MatchCommand bulkMatchStatus(String rootuid) {
        String url = constructUrl("/bulk/{rootuid}", rootuid);
        // not tracing request that commonly used in polling
        return getNoTracing("bulk_status", url, MatchCommand.class);
    }

    public EntityPublishStatistics publishEntity(EntityPublishRequest request) {
        String url = constructUrl("/entity/publish");
        return postKryo("publish_entity", url, request, EntityPublishStatistics.class);
    }

    public List<EntityPublishStatistics> publishEntity(List<EntityPublishRequest> requests) {
        String url = constructUrl("/entity/publish/list");
        List<?> list = postKryo("publish_entity_list", url, requests, List.class);
        return JsonUtils.convertList(list, EntityPublishStatistics.class);
    }

    public BumpVersionResponse bumpVersion(BumpVersionRequest request) {
        String url = constructUrl("/entity/versions");
        return postKryo("bump_version", url, request, BumpVersionResponse.class);
    }

    public BumpVersionResponse bumpNextVersion(BumpVersionRequest request) {
        String url = constructUrl("/entity/versions/next");
        return postKryo("bump_next_version", url, request, BumpVersionResponse.class);
    }

    public SimpleBooleanResponse saveEntityMatchConfiguration(@NotNull String customerSpace,
            @NotNull EntityMatchConfiguration configuration) {
        String url = constructUrl("/entity/configurations");
        ConfigurationRequest request = new ConfigurationRequest();
        request.setTenant(new Tenant(customerSpace));
        request.setConfiguration(configuration);
        return postKryo("save_entity_match_configuration", url, request, SimpleBooleanResponse.class);
    }

    public EntityMatchConfiguration getEntityMatchConfiguration(@NotNull String customerSpace) {
        String url = constructUrl("/entity/configurations/{customerSpace}",
                CustomerSpace.shortenCustomerSpace(customerSpace));
        return get("save_entity_match_configuration", url, EntityMatchConfiguration.class);
    }

    public Map<EntityMatchEnvironment, EntityMatchVersion> getEntityMatchVersions(String customerSpace,
            boolean clearCache) {
        String url = constructUrl("/entity/versions/{customerSpace}?{clearCache}", customerSpace, clearCache);
        Map<?, ?> map = get("get_entity_match_versions", url, Map.class);
        return JsonUtils.convertMap(map, EntityMatchEnvironment.class, EntityMatchVersion.class);
    }

    public EntityMatchVersion getEntityMatchVersion(@NotNull String customerSpace, @NotNull EntityMatchEnvironment env,
            boolean clearCache) {
        String url = constructUrl("/entity/versions/{customerSpace}/{environment}?{clearCache}", customerSpace,
                env.name(), clearCache);
        return get("get_entity_match_version", url, EntityMatchVersion.class);
    }

    public String lookupInternalAccountId(@NotNull String customerSpace, @NotNull String lookupId,
            @NotNull String lookupIdVal, DataCollection.Version version) {
        String url = constructUrl("/cdllookup", customerSpace);
        InternalAccountIdLookupRequest request = new InternalAccountIdLookupRequest();
        request.setCustomerSpace(customerSpace);
        request.setLookupId(lookupId);
        request.setLookupIdVals(Collections.singletonList(lookupIdVal));
        request.setDataCollectionVersion(version);
        return post("lookup_internal_account_id", url, request, String.class);
    }

    public List<String> lookupInternalAccountIds(@NotNull String customerSpace, @NotNull String lookupId,
                                          @NotNull List<String> lookupIdVals, DataCollection.Version version) {
        String url = constructUrl("/cdllookup/accountids", customerSpace);
        InternalAccountIdLookupRequest request = new InternalAccountIdLookupRequest();
        request.setCustomerSpace(customerSpace);
        request.setLookupId(lookupId);
        request.setLookupIdVals(lookupIdVals);
        request.setDataCollectionVersion(version);
        return post("lookup_internal_account_id", url, request, List.class);
    }


    public List<Map<String, Object>> lookupContacts(@NotNull String customerSpace, @NotNull String lookupId,
            @NotNull String lookupIdVal, String contactId, DataCollection.Version version) {
        String url = constructUrl("/cdllookup/contacts", customerSpace);
        InternalContactLookupRequest request = new InternalContactLookupRequest();
        request.setCustomerSpace(customerSpace);
        request.setContactId(contactId);
        request.setAccountLookupId(lookupId);
        request.setAccountLookupIdVal(lookupIdVal);
        request.setDataCollectionVersion(version);
        List<?> raw = post("lookup_contacts_by_account_id", url, request, List.class);
        return JsonUtils.convertListOfMaps(raw, String.class, Object.class);
    }

    public Map<String, Object> lookupAccount(@NotNull String customerSpace,
                                             @NotNull String indexName,
                                             @NotNull String lookupId,
                                             @NotNull String lookupIdVal) {
        String url = constructUrl("/cdllookup/account", customerSpace);
        InternalAccountLookupRequest request = new InternalAccountLookupRequest();
        request.setCustomerSpace(customerSpace);
        request.setIndexName(indexName);
        request.setLookupIdKey(lookupId);
        request.setLookupIdValue(lookupIdVal);
        Map raw = post("lookup_account", url, request, Map.class);
        return JsonUtils.convertMap(raw, String.class, Object.class);
    }
}
