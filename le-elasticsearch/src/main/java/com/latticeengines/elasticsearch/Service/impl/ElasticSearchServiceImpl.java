package com.latticeengines.elasticsearch.Service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;

import parquet.Preconditions;

@Service("elasticSearchService")
public class ElasticSearchServiceImpl implements ElasticSearchService {

    private static Logger log = LoggerFactory.getLogger(ElasticSearchServiceImpl.class);

    private static final int MAX_RETRY = 3;

    @Inject
    private RestHighLevelClient client;

    @Value("${elasticsearch.shards}")
    private int esShards;
    @Value("${elasticsearch.replicas}")
    private int esReplicas;
    @Value("${elasticsearch.refreshinterval}")
    private String esRefreshInterval;
    @Value("${elasticsearch.dynamic}")
    private boolean esDynamic;
    @Value("${elasticsearch.http.scheme}")
    private String esHttpScheme;
    @Value("${elasticsearch.host}")
    private String esHost;
    @Value("${elasticsearch.ports}")
    private String esPorts;
    @Value("${elasticsearch.user}")
    private String user;
    @Value("${elasticsearch.pwd.encrypted}")
    private String password;

    @Override
    public boolean createIndex(String indexName, String esEntityType) {


        return createIndexWithSettings(indexName, getDefaultElasticSearchConfig(), esEntityType);
    }

    @Override
    public boolean createIndexWithSettings(String indexName, ElasticSearchConfig esConfig, String esEntityType) {
        Preconditions.checkNotNull(indexName, "index Name can't be null.");
        Preconditions.checkNotNull(esConfig, "esConfig can't be null");
        Preconditions.checkNotNull(esEntityType, "esEntityType can't be null.");
        if (esConfig.getRefreshInterval() == null) {
            esConfig.setRefreshInterval(esRefreshInterval);
        }
        if (esConfig.getReplicas() == null) {
            esConfig.setReplicas(esReplicas);
        }
        if (esConfig.getShards() == null) {
            esConfig.setShards(esShards);
        }
        if (esConfig.getDynamic() == null) {
            esConfig.setDynamic(esDynamic);
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createIndexWithSettings(client, indexName, esConfig,
                    ElasticSearchUtils.initIndexMapping(esEntityType, esConfig.getDynamic()));
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create index %s, xContentBuilderType is %s", indexName, esEntityType), e);
            return false;
        }
        return true;
    }



    @Override
    public boolean createDocument(String indexName, String docId, String jsonString) {
        Preconditions.checkNotNull(indexName, "indexName can't be null.");
        Preconditions.checkNotNull(docId, "docId can't be null.");
        Preconditions.checkNotNull(jsonString, "jsonString can't be null.");
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createDocument(client, indexName, docId, jsonString);
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create document %s", docId), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean createDocuments(String indexName, Map<String, String> docs) {
        Preconditions.checkNotNull(indexName, "indexName can't be null.");
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createDocuments(client, indexName, docs);
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create documents, docIds is %s.", docs.keySet().toString()), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean createAccountIndexWithLookupIds(String indexName, ElasticSearchConfig esConfig,
                                                   List<String> lookupIds) {
        Preconditions.checkNotNull(indexName, "index Name can't be null.");
        Preconditions.checkNotNull(esConfig, "esConfig can't be null");
        if (esConfig.getRefreshInterval() == null) {
            esConfig.setRefreshInterval(esRefreshInterval);
        }
        if (esConfig.getReplicas() == null) {
            esConfig.setReplicas(esReplicas);
        }
        if (esConfig.getShards() == null) {
            esConfig.setShards(esShards);
        }
        if (esConfig.getDynamic() == null) {
            esConfig.setDynamic(esDynamic);
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createIndexWithSettings(client, indexName, esConfig,
                    ElasticSearchUtils.initAccountIndexMapping(esConfig.getDynamic(), lookupIds));
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create account index %s", indexName), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean checkFieldExist(String indexName, String fieldName) {
        try {
            return ElasticSearchUtils.checkFieldExist(client, indexName, fieldName);
        } catch (IOException e) {
            log.error("error when check the mapping exist", e);
            return false;
        }
    }

    @Override
    public Map<String, Object> getSourceMapping(String indexName) {
        try {
            return ElasticSearchUtils.getSourceMapping(client, indexName);
        } catch (IOException e) {
            log.error("error web get source mapping", e);
            return null;
        }
    }

    @Override
    public boolean updateIndexMapping(String indexName, String fieldName, String type) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {
            retry.execute(context -> {
                ElasticSearchUtils.updateIndexMapping(client, indexName, fieldName, type);
                return 0;
            });
        } catch (IOException e) {
            log.error("failed to update mapping for index {}  field {}", indexName, fieldName, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean updateAccountIndexMapping(String indexName, String fieldName, String type, List<String> lookupIds,
                                             String subType) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {
            retry.execute(context -> {
                ElasticSearchUtils.updateAccountIndexMapping(client, indexName, fieldName, type, lookupIds, subType);
                return 0;
            });
        } catch (IOException e) {
            log.error("failed to update mapping for index {}  field {}", indexName, fieldName, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteIndex(String indexName) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {
            retry.execute(context -> {
                ElasticSearchUtils.deleteIndex(client, indexName);
                return 0;
            });
        } catch(IOException e) {
            log.error("failed to delete index {}", indexName);
            return false;
        }
        return true;
    }

    @Override
    public boolean indexExists(String indexName) {
        try {
            return ElasticSearchUtils.indexExists(client, indexName);
        } catch (IOException e) {
            log.error("error occur when checking index {}", indexName);
            return false;
        }
    }

    @Override
    public ElasticSearchConfig getDefaultElasticSearchConfig() {
        ElasticSearchConfig elasticSearchConfig = new ElasticSearchConfig();
        elasticSearchConfig.setShards(esShards);
        elasticSearchConfig.setReplicas(esReplicas);
        elasticSearchConfig.setRefreshInterval(esRefreshInterval);
        elasticSearchConfig.setDynamic(esDynamic);
        elasticSearchConfig.setEsHost(esHost);
        elasticSearchConfig.setEsPort(esPorts);
        elasticSearchConfig.setEsUser(user);
        elasticSearchConfig.setEsPassword(password);
        elasticSearchConfig.setHttpScheme(esHttpScheme);
        return elasticSearchConfig;
    }

    @Override
    public Map<String, Object> searchByAccountId(String indexName, String lookupIdValue) {
        Preconditions.checkNotNull(indexName, "index Name can't be null.");
        Preconditions.checkNotNull(lookupIdValue, "lookupIdValue can't be null");
        Map<String, Object> data = new HashMap<>();
        GetResponse response = ElasticSearchUtils.searchByAccountId(client, indexName, lookupIdValue);
        if (response.isExists()) {
            return stripPrefixes(response.getSourceAsMap());
        } else {
            return data;
        }
    }

    @Override
    public Map<String, Object> searchByLookupId(String indexName, String lookupIdKey, String lookupIdValue) {
        Preconditions.checkNotNull(indexName, "index Name can't be null.");
        Preconditions.checkNotNull(lookupIdKey, "lookupIdKey can't be null");
        Preconditions.checkNotNull(lookupIdValue, "lookupIdValue can't be null");
        Map<String, Object> data = new HashMap<>();
        SearchResponse response = ElasticSearchUtils.searchByLookupId(client, indexName, lookupIdKey, lookupIdValue);
        if (response == null) {
            log.error("Can't get response using lookupIdValue {} and indexName {}.", lookupIdValue, indexName);
            return data;
        }
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            data.putAll(stripPrefixes(hit.getSourceAsMap()));
        }
        return data;
    }

    @Override
    public List<Map<String, Object>> searchTimelineByEntityIdAndDateRange(String indexName, String entity,
                                                                          String entityId, Long fromDate, Long toDate) {
        Preconditions.checkNotNull(indexName, "index name should not be null");
        Preconditions.checkNotNull(entity, "BusinessEntity should not be null");
        Preconditions.checkNotNull(entityId, "entity id value should not be null");
        Preconditions.checkNotNull(fromDate, "fromDate should not be null");
        Preconditions.checkNotNull(toDate, "toDate should not be null");
        List<Map<String, Object>> data = new ArrayList<>();
        SearchResponse response = ElasticSearchUtils.searchTimelineByEntityIdAndDateRange(client, indexName, entity,
                entityId, fromDate, toDate);
        if (response == null) {
            return data;
        }
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            data.add(stripPrefixes(hit.getSourceAsMap()));
        }
        return data;
    }

    @Override
    public List<Map<String, Object>> searchContactByContactId(String indexName, String contactId) {
        Preconditions.checkNotNull(indexName, "index name should not be null");
        Preconditions.checkNotNull(contactId, "contactId should not be null");
        List<Map<String, Object>> data = new ArrayList<>();
        GetResponse response = ElasticSearchUtils.searchContactByContactId(client, indexName, contactId);
        if (response.isExists()) {
            data.add(stripPrefixes(response.getSourceAsMap()));
        }
        return data;
    }

    @Override
    public List<Map<String, Object>> searchContactByAccountId(String indexName, String internalAccountId) {
        Preconditions.checkNotNull(indexName, "index name should not be null");
        Preconditions.checkNotNull(internalAccountId, "internalAccountId should not be null");
        List<Map<String, Object>> data = new ArrayList<>();
        SearchResponse response = ElasticSearchUtils.searchContactByAccountId(client, indexName, internalAccountId);
        if (response == null) {
            return data;
        }
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            data.add(stripPrefixes(hit.getSourceAsMap()));
        }
        return data;
    }

    @Override
    public String searchAccountIdByLookupId(String indexName, String lookupIdKey, String lookupIdValue) {
        Preconditions.checkNotNull(indexName, "index name should not be null");
        Preconditions.checkNotNull(lookupIdKey, "lookupIdKey should not be null");
        Preconditions.checkNotNull(lookupIdValue, "lookup id value should not be null");
        return ElasticSearchUtils.searchAccountIdByLookupId(client, indexName, lookupIdKey, lookupIdValue);
    }

    private Map<String, Object> stripPrefixes(Map<String, Object> vals) {
        return vals //
                .entrySet() //
                .stream() //
                .map(entry -> Pair.of(stripPrefix(entry.getKey()), entry.getValue())) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
    }

    private String stripPrefix(String key) {
        return key.substring(key.indexOf(":") + 1);
    }
}
