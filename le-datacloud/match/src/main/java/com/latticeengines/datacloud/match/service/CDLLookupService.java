package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

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

    boolean clearAccountLookupDUCache();

    List<Map<String, Object>> lookupContactsByInternalAccountId(String customerSpace, DataCollection.Version version,
            String lookupIdKey, String lookupIdValue, String contactId);

}
