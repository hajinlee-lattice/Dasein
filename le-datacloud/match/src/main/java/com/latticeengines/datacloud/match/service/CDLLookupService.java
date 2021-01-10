package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;

public interface CDLLookupService {

    List<ColumnMetadata> parseMetadata(MatchInput input);

    DynamoDataUnit parseAccountLookupDataUnit(MatchInput input);

    List<DynamoDataUnit> parseCustomDynamoDataUnits(MatchInput input);

    Map<String, Object> lookup(DynamoDataUnit lookupDataUnit, List<DynamoDataUnit> dynamoDataUnits, String lookupIdKey,
            String lookupIdValue);

    // using new account lookup method
    String lookupInternalAccountId(String customerSpace, DataCollection.Version version, String lookupIdKey, String lookupIdValue);

    List<String> lookupInternalAccountIds(String customerSpace, DataCollection.Version version, String lookupIdKey,
                                     List<String> lookupIdValues);

    boolean clearAccountLookupDUCache();

    List<Map<String, Object>> lookupContactsByInternalAccountId(String customerSpace, DataCollection.Version version,
            String lookupIdKey, String lookupIdValue, String contactId);

    String lookupInternalAccountIdByEs(@NotNull String customerSpace, @NotNull String indexName,
            @NotNull String lookupIdKey, @NotNull String lookupIdValue);

    List<Map<String, Object>> lookupContactsByESInternalAccountId(String customerSpace,
            @NotNull String indexName, String accountIndexName, String lookupIdKey, String lookupIdValue,
                                                                  String contactId);

    Map<String, Object> lookup(@NotNull String customerSpace, @NotNull String indexName,
                               String lookupIdKey, String lookupIdValue);

    List<Map<String, Object>> searchTimelineByES(@NotNull String customerSpace, String indexName,
                                            String entity, String entityId, Long fromDate, Long toDate);
}
