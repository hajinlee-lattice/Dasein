package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;

public interface CDLLookupService {

    List<ColumnMetadata> parseMetadata(MatchInput input);

    DynamoDataUnit parseCustomAccountDynamo(MatchInput input);

    List<DynamoDataUnit> parseCustomDynamo(MatchInput input);

    Map<String, Object> lookup(List<DynamoDataUnit> dynamoDataUnits, String lookupIdKey, String lookupIdValue);

}
