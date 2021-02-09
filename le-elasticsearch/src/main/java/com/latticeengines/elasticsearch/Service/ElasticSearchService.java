package com.latticeengines.elasticsearch.Service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;

public interface ElasticSearchService {

    boolean createIndex(String indexName, String esEntityType);

    boolean createIndexWithSettings(String indexName, ElasticSearchConfig esConfig, String esEntityType);

    boolean createDocument(String indexName, String docId, String jsonString);

    boolean createDocuments(String indexName, Map<String, String> docs);

    boolean createAccountIndexWithLookupIds(String indexName, ElasticSearchConfig esConfig,
                                            List<String> lookupIds);

    boolean checkFieldExist(String indexName, String fieldName);

    Map<String, Object> getSourceMapping(String indexName);

    boolean updateIndexMapping(String indexName, String fieldName, String type);

    boolean updateAccountIndexMapping(String indexName, String fieldName, String type, List<String> lookupIds,
                                      String subType);

    boolean deleteIndex(String indexName);

    boolean indexExists(String indexName);

    Map<String, Object> searchByAccountId(String indexName, String lookupIdValue);

    Map<String, Object> searchByLookupId(String indexName, String lookupIdKey, String lookupIdValue);

    List<Map<String, Object>> searchTimelineByEntityIdAndDateRange(String indexName, String entity,
                                                                   String entityId, Long fromDate, Long toDate);

    List<Map<String, Object>> searchContactByContactId(String indexName, String contactId);

    List<Map<String, Object>> searchContactByAccountId(String indexName, String internalAccountId);

    String searchAccountIdByLookupId(String indexName, String lookupIdKey, String lookupIdValue);

    ElasticSearchConfig getDefaultElasticSearchConfig();
}
