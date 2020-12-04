package com.latticeengines.elasticsearch.Service;

import java.util.Map;

import com.latticeengines.elasticsearch.config.ElasticSearchConfig;

public interface ElasticSearchService {

    boolean createIndex(String indexName);

    boolean createIndexWithSettings(String indexName, ElasticSearchConfig esConfig);

    boolean createDocument(String indexName, String docId, String jsonString);

    boolean createDocuments(String indexName, Map<String, String> docs);

    ElasticSearchConfig getDefaultElasticSearchConfig();
}
